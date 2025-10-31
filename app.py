import requests
import json
import time
from datetime import datetime, timedelta
import os
import logging
import re
import hashlib

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
        
        self.last_log_hashes = {}
        self.last_log_count = {}
    
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
    
    def format_log_timestamp(self, log_line):
        """
        Extract and format timestamp from log line
        Example: frontend-customer-production 2025-10-27T19:50:32.682366470Z Listening on...
        Returns: (formatted_time, remaining_log)
        """
        pattern = r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)'
        match = re.search(pattern, log_line)
        
        if match:
            timestamp_str = match.group(1)
            try:
                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S')
                cleaned_line = re.sub(pattern, '', log_line).strip()
                return formatted_time, cleaned_line
            except Exception as e:
                logger.debug(f"Error parsing timestamp: {e}")
                return None, log_line
        
        return None, log_line
    
    def get_new_logs_only(self, log_lines, app_id, component_name):
        """
        Get only NEW logs that haven't been sent before
        Uses hash-based tracking of last N lines
        """
        key = f"{app_id}:{component_name}"
        
        if not log_lines:
            return []
        
        
        last_n = 50
        recent_logs = log_lines[-last_n:] if len(log_lines) > last_n else log_lines
        current_hash = hashlib.md5('\n'.join(recent_logs).encode()).hexdigest()
        
        
        if key not in self.last_log_hashes:
            logger.info(f"🆕 First run for {component_name} - storing baseline ({len(log_lines)} lines)")
            self.last_log_hashes[key] = current_hash
            self.last_log_count[key] = len(log_lines)
            return []  
        
        
        if self.last_log_hashes[key] == current_hash and self.last_log_count[key] == len(log_lines):
            logger.info(f"⏭️  No new logs for {component_name}")
            return []
        
        
        last_count = self.last_log_count[key]
        new_logs = []
        
        if len(log_lines) > last_count:
            
            new_logs = log_lines[last_count:]
            logger.info(f"📊 Found {len(new_logs)} new log lines (out of {len(log_lines)} total)")
        elif len(log_lines) < last_count:
            
            new_logs = log_lines
            logger.info(f"🔄 App restart detected - sending all {len(new_logs)} logs")
        else:
            
            new_logs = log_lines[-last_n:]
            logger.info(f"🔄 Logs changed - sending last {len(new_logs)} lines")
        
        
        self.last_log_hashes[key] = current_hash
        self.last_log_count[key] = len(log_lines)
        
        return new_logs
    
    def format_for_loki(self, log_lines, app_id, component_name, app_name=""):
        """Format logs for Loki push API"""
        if not log_lines:
            return None
        
        values = []
        for line in log_lines:
            formatted_time, cleaned_line = self.format_log_timestamp(line)
            
            if formatted_time:
                pretty_log = f"[{formatted_time}] {cleaned_line}"
            else:
                pretty_log = line
            
            timestamp_ns = str(int(time.time() * 1e9))
            values.append([timestamp_ns, pretty_log])
        
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
    
    def batch_logs(self, log_lines, batch_size=1000):
        """
        Split log lines into batches
        Returns: list of batches
        """
        batches = []
        for i in range(0, len(log_lines), batch_size):
            batch = log_lines[i:i + batch_size]
            batches.append(batch)
        return batches
    
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
    
    def forward_logs(self, app_id, component_name, app_name="", log_type="RUN", batch_size=1000):
        """Main method to fetch and forward logs"""
        logger.info(f"🔄 Fetching logs for {app_name or app_id}/{component_name}...")
        
        logs = self.get_app_logs(app_id, component_name, log_type=log_type)
        
        if logs:
            all_log_lines = self.parse_log_lines(logs)
            
            if all_log_lines:
                logger.info(f"📋 Total logs from API: {len(all_log_lines)}")
                
                
                new_logs = self.get_new_logs_only(all_log_lines, app_id, component_name)
                
                if not new_logs:
                    return False
                
                logger.info(f"📝 Processing {len(new_logs)} new log lines")
                
                
                if len(new_logs) > batch_size:
                    logger.info(f"📦 Splitting into batches of {batch_size} logs...")
                    batches = self.batch_logs(new_logs, batch_size)
                    
                    success_count = 0
                    for i, batch in enumerate(batches, 1):
                        logger.info(f"📤 Pushing batch {i}/{len(batches)} ({len(batch)} logs)...")
                        formatted_logs = self.format_for_loki(batch, app_id, component_name, app_name)
                        
                        if formatted_logs and self.push_to_loki(formatted_logs):
                            success_count += 1
                        
                        
                        if i < len(batches):
                            time.sleep(0.5)
                    
                    logger.info(f"✅ Pushed {success_count}/{len(batches)} batches successfully")
                    return success_count > 0
                else:
                    
                    formatted_logs = self.format_for_loki(new_logs, app_id, component_name, app_name)
                    
                    if formatted_logs:
                        logger.info("📤 Pushing logs to Loki...")
                        return self.push_to_loki(formatted_logs)
                    else:
                        logger.warning("⚠️  No logs to format")
                        return False
            else:
                logger.info(f"ℹ️  No log lines found for {app_name or app_id}/{component_name}")
                return False
        else:
            logger.error(f"❌ Failed to fetch logs for {app_name or app_id}/{component_name}")
            return False
    
    def run_continuous_multi(self, apps_config, interval=60, log_type="RUN", batch_size=1000):
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
        logger.info(f"📦 Batch size: {batch_size} logs per request")
        logger.info(f"📝 Note: First run will NOT send historical logs (baseline only)")
        
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
                        self.forward_logs(app_id, component_name, app_name, log_type, batch_size)
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
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))  
    
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
    logger.info(f"📦 Batch Size: {BATCH_SIZE}")
    
    forwarder = AppPlatformToLoki(
        do_token=DO_TOKEN,
        loki_url=LOKI_URL,
        loki_user=LOKI_USER,
        loki_password=LOKI_PASSWORD
    )
    
    forwarder.run_continuous_multi(apps_config, interval=INTERVAL, log_type=LOG_TYPE, batch_size=BATCH_SIZE)