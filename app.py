import requests
import json
import time
from datetime import datetime, timedelta
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AppPlatformToLoki:
    def __init__(self, do_token, loki_url, loki_user, loki_password):
        self.do_token = do_token
        self.loki_url = loki_url
        self.loki_auth = (loki_user, loki_password) if loki_user else None
        self.do_headers = {
            "Authorization": f"Bearer {do_token}",
            "Content-Type": "application/json"
        }
    
    def get_app_logs(self, app_id, component_name, log_type="RUN"):
        """Fetch logs from DigitalOcean App Platform"""
        url = f"https://api.digitalocean.com/v2/apps/{app_id}/components/{component_name}/logs"
        
        params = {
            "type": log_type,
            "follow": "false"
        }
        
        try:
            response = requests.get(url, headers=self.do_headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Fetched logs from DO App Platform: {app_id}/{component_name}")
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching logs from App Platform {app_id}/{component_name}: {e}")
            return None
    
    def parse_log_lines(self, logs_data):
        """Parse log lines from DO response"""
        log_lines = []
        
        if not logs_data:
            return log_lines
        
        
        live_url = logs_data.get("live_url")
        if live_url:
            try:
                response = requests.get(live_url, timeout=30)
                response.raise_for_status()
                
                for line in response.text.strip().split('\n'):
                    if line:
                        log_lines.append(line)
            except Exception as e:
                logger.error(f"Error fetching live logs: {e}")
        
        return log_lines
    
    def format_for_loki(self, log_lines, app_id, component_name, app_name=""):
        """Format logs for Loki push API"""
        if not log_lines:
            return None
        
        values = []
        for line in log_lines:
            timestamp_ns = str(int(time.time() * 1e9))
            values.append([timestamp_ns, line])
        
        if not values:
            return None
        
        stream = {
            "stream": {
                "app_id": app_id,
                "component": component_name,
                "app_name": app_name,
                "source": "digitalocean_app_platform",
                "job": "app_logs"
            },
            "values": values
        }
        
        return {"streams": [stream]}
    
    def push_to_loki(self, formatted_logs):
        """Push formatted logs to Loki"""
        if not formatted_logs:
            return False
        
        try:
            headers = {
                "Content-Type": "application/json",
                "X-Scope-OrgID": "fake"
            }
            
            response = requests.post(
                self.loki_url,
                json=formatted_logs,
                auth=self.loki_auth,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            
            total_logs = sum(len(s['values']) for s in formatted_logs['streams'])
            logger.info(f"✅ Successfully pushed {total_logs} log lines to Loki")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error pushing to Loki: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            return False
    
    def forward_logs(self, app_id, component_name, app_name="", log_type="RUN"):
        """Main method to fetch and forward logs"""
        logger.info(f"🔄 Fetching logs for {app_name or app_id}/{component_name}...")
        
        logs = self.get_app_logs(app_id, component_name, log_type=log_type)
        
        if logs:
            log_lines = self.parse_log_lines(logs)
            
            if log_lines:
                logger.info(f"📝 Parsed {len(log_lines)} log lines")
                formatted_logs = self.format_for_loki(log_lines, app_id, component_name, app_name)
                
                if formatted_logs:
                    logger.info("📤 Pushing logs to Loki...")
                    return self.push_to_loki(formatted_logs)
                else:
                    logger.warning("⚠️  No logs to format")
                    return False
            else:
                logger.info(f"ℹ️  No new log lines found for {app_name or app_id}/{component_name}")
                return False
        else:
            logger.error(f"❌ Failed to fetch logs for {app_name or app_id}/{component_name}")
            return False
    
    def run_continuous_multi(self, apps_config, interval=60, log_type="RUN"):
        """
        Run continuously, forwarding logs from multiple apps/components
        
        apps_config format:
        [
            {"app_id": "xxx", "component_name": "yyy", "app_name": "prod"},
            {"app_id": "zzz", "component_name": "www", "app_name": "staging"}
        ]
        """
        logger.info(f"🚀 Starting continuous log forwarding for {len(apps_config)} app(s)...")
        logger.info(f"⏱️  Interval: {interval}s")
        
        for config in apps_config:
            logger.info(f"   📱 {config.get('app_name', 'Unknown')}: {config['app_id']}/{config['component_name']}")
        
        while True:
            try:
                logger.info(f"\n{'='*60}")
                logger.info(f"🔄 Starting new collection cycle - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"{'='*60}")
                
                for config in apps_config:
                    app_id = config['app_id']
                    component_name = config['component_name']
                    app_name = config.get('app_name', '')
                    
                    try:
                        self.forward_logs(app_id, component_name, app_name, log_type)
                    except Exception as e:
                        logger.error(f"❌ Error processing {app_name or app_id}/{component_name}: {e}")
                        continue
                
                logger.info(f"\n⏱️  Waiting {interval} seconds until next cycle...")
                time.sleep(interval)
                
            except KeyboardInterrupt:
                logger.info("\n⛔ Stopping log forwarder...")
                break
            except Exception as e:
                logger.error(f"❌ Error in continuous run: {e}")
                time.sleep(interval)


def parse_apps_config(apps_string):
    """
    Parse apps configuration from environment variable
    Format: app_id1:component1:name1,app_id2:component2:name2
    Example: xxx:frontend:prod,yyy:backend:staging
    """
    apps = []
    
    for app_config in apps_string.split(','):
        parts = app_config.strip().split(':')
        if len(parts) >= 2:
            config = {
                'app_id': parts[0].strip(),
                'component_name': parts[1].strip(),
                'app_name': parts[2].strip() if len(parts) > 2 else ''
            }
            apps.append(config)
    
    return apps


if __name__ == "__main__":
    
    DO_TOKEN = os.getenv("DO_TOKEN")
    
    
    
    APPS_CONFIG = os.getenv("APPS_CONFIG")
    
    
    APP_ID = os.getenv("APP_ID")
    COMPONENT_NAME = os.getenv("COMPONENT_NAME")
    
    LOKI_URL = os.getenv("LOKI_URL")
    LOKI_USER = os.getenv("LOKI_USER", "")
    LOKI_PASSWORD = os.getenv("LOKI_PASSWORD", "")
    
    INTERVAL = int(os.getenv("INTERVAL", "60"))
    LOG_TYPE = os.getenv("LOG_TYPE", "RUN")
    
    
    if not DO_TOKEN or not LOKI_URL:
        logger.error("❌ Missing required environment variables: DO_TOKEN, LOKI_URL")
        exit(1)
    
    
    apps_config = []
    
    if APPS_CONFIG:
        
        apps_config = parse_apps_config(APPS_CONFIG)
        logger.info(f"✅ Multi-app mode: {len(apps_config)} app(s) configured")
    elif APP_ID and COMPONENT_NAME:
        
        apps_config = [{
            'app_id': APP_ID,
            'component_name': COMPONENT_NAME,
            'app_name': 'default'
        }]
        logger.info("✅ Single-app mode")
    else:
        logger.error("❌ No apps configured. Set either APPS_CONFIG or APP_ID+COMPONENT_NAME")
        exit(1)
    
    logger.info(f"📍 Loki URL: {LOKI_URL}")
    logger.info(f"⏱️  Interval: {INTERVAL}s")
    logger.info(f"📋 Log Type: {LOG_TYPE}")
    
    
    forwarder = AppPlatformToLoki(
        do_token=DO_TOKEN,
        loki_url=LOKI_URL,
        loki_user=LOKI_USER,
        loki_password=LOKI_PASSWORD
    )
    
    
    forwarder.run_continuous_multi(apps_config, interval=INTERVAL, log_type=LOG_TYPE)