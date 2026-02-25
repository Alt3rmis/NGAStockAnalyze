#!/usr/bin/env python3
"""
GitHub Webhook 处理服务
监听 GitHub Push 事件，自动触发部署

使用方法:
    python webhook_server.py --port 9000 --secret YOUR_WEBHOOK_SECRET

配置:
    1. 在 GitHub 仓库设置中添加 Webhook
    2. URL: http://your-server:9000/webhook
    3. Secret: 与启动参数一致
    4. Content type: application/json
    5. Events: Just the push event
"""

import argparse
import hashlib
import hmac
import json
import logging
import os
import subprocess
import sys
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('/opt/NGAStockAnalyze/logs/webhook.log')
    ]
)
logger = logging.getLogger(__name__)

DEPLOY_SCRIPT = "/opt/NGAStockAnalyze/deploy/deploy.sh"
PROJECT_DIR = "/opt/NGAStockAnalyze"
ALLOWED_BRANCHES = ["main", "master", "develop"]
DEPLOY_LOG = "/opt/NGAStockAnalyze/logs/deploy_history.log"


class WebhookHandler(BaseHTTPRequestHandler):
    webhook_secret = None
    
    def log_message(self, format, *args):
        logger.info("%s - %s", self.address_string(), format % args)
    
    def send_json_response(self, status_code, data):
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode('utf-8'))
    
    def verify_signature(self, payload, signature_header):
        if not self.webhook_secret:
            return True
        
        if not signature_header:
            return False
        
        hash_object = hmac.new(
            self.webhook_secret.encode('utf-8'),
            msg=payload,
            digestmod=hashlib.sha256
        )
        expected_signature = "sha256=" + hash_object.hexdigest()
        
        return hmac.compare_digest(expected_signature, signature_header)
    
    def do_GET(self):
        if self.path == '/health':
            self.send_json_response(200, {"status": "healthy", "timestamp": datetime.now().isoformat()})
        elif self.path == '/status':
            self.send_json_response(200, {"status": "running", "service": "webhook-server"})
        else:
            self.send_json_response(404, {"error": "Not found"})
    
    def do_POST(self):
        if self.path != '/webhook':
            self.send_json_response(404, {"error": "Not found"})
            return
        
        content_length = int(self.headers.get('Content-Length', 0))
        if content_length == 0:
            self.send_json_response(400, {"error": "Empty payload"})
            return
        
        payload = self.rfile.read(content_length)
        
        signature = self.headers.get('X-Hub-Signature-256', '')
        if not self.verify_signature(payload, signature):
            logger.warning("签名验证失败")
            self.send_json_response(401, {"error": "Invalid signature"})
            return
        
        try:
            data = json.loads(payload.decode('utf-8'))
        except json.JSONDecodeError:
            self.send_json_response(400, {"error": "Invalid JSON"})
            return
        
        event_type = self.headers.get('X-GitHub-Event', '')
        
        if event_type == 'push':
            self.handle_push_event(data)
        elif event_type == 'ping':
            self.send_json_response(200, {"message": "pong"})
        else:
            self.send_json_response(200, {"message": f"Event {event_type} ignored"})
    
    def handle_push_event(self, data):
        ref = data.get('ref', '')
        branch = ref.replace('refs/heads/', '')
        
        if branch not in ALLOWED_BRANCHES:
            logger.info(f"分支 {branch} 不在允许列表中，跳过部署")
            self.send_json_response(200, {"message": f"Branch {branch} ignored"})
            return
        
        pusher = data.get('pusher', {}).get('name', 'unknown')
        commits = data.get('commits', [])
        commit_count = len(commits)
        
        logger.info(f"收到推送: 分支={branch}, 推送者={pusher}, 提交数={commit_count}")
        
        self.send_json_response(200, {
            "message": "Deployment triggered",
            "branch": branch,
            "pusher": pusher,
            "commits": commit_count
        })
        
        self.trigger_deployment(branch, pusher)
    
    def trigger_deployment(self, branch, pusher):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            os.chdir(PROJECT_DIR)
            
            result = subprocess.run(
                [DEPLOY_SCRIPT, "update"],
                capture_output=True,
                text=True,
                timeout=300
            )
            
            with open(DEPLOY_LOG, 'a', encoding='utf-8') as f:
                f.write(f"\n{'='*50}\n")
                f.write(f"[{timestamp}] 部署触发\n")
                f.write(f"分支: {branch}\n")
                f.write(f"推送者: {pusher}\n")
                f.write(f"更新输出:\n{result.stdout}\n")
                if result.stderr:
                    f.write(f"错误输出:\n{result.stderr}\n")
                f.write(f"返回码: {result.returncode}\n")
            
            if result.returncode == 0:
                logger.info("代码更新成功，重启服务...")
                subprocess.run([DEPLOY_SCRIPT, "restart"], check=True)
                logger.info("部署完成")
            else:
                logger.error(f"更新失败: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            logger.error("部署超时")
        except Exception as e:
            logger.error(f"部署异常: {e}")


def run_server(port, secret):
    WebhookHandler.webhook_secret = secret
    
    server_address = ('', port)
    httpd = HTTPServer(server_address, WebhookHandler)
    
    logger.info(f"Webhook 服务启动，监听端口 {port}")
    logger.info(f"Webhook URL: http://your-server:{port}/webhook")
    logger.info(f"健康检查: http://your-server:{port}/health")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("服务停止")
        httpd.shutdown()


def main():
    parser = argparse.ArgumentParser(description='GitHub Webhook 部署服务')
    parser.add_argument('--port', type=int, default=9000, help='监听端口')
    parser.add_argument('--secret', type=str, default=None, help='Webhook Secret')
    
    args = parser.parse_args()
    
    if args.secret is None:
        args.secret = os.environ.get('WEBHOOK_SECRET', '')
    
    if not args.secret:
        logger.warning("未设置 Webhook Secret，将跳过签名验证（不安全）")
    
    run_server(args.port, args.secret)


if __name__ == '__main__':
    main()
