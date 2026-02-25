"""
Automated Notification System
Supports multiple delivery channels: Email, WeChat Bot, QQ Bot
With retry mechanism, error handling, and logging
"""

import json
import logging
import smtplib
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union
from functools import wraps
import hashlib

BEIJING_TZ = timezone(timedelta(hours=8))

LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOGS_DIR / 'notifier.log', encoding='utf-8')
    ]
)
logger = logging.getLogger('Notifier')


class NotificationChannel(Enum):
    """Available notification channels"""
    EMAIL = "email"
    WECHAT = "wechat"
    QQ = "qq"


class NotificationStatus(Enum):
    """Status of notification delivery"""
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"
    RETRY = "retry"


@dataclass
class NotificationResult:
    """Result of a notification attempt"""
    channel: NotificationChannel
    status: NotificationStatus
    message: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(BEIJING_TZ))
    retry_count: int = 0
    error_details: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "channel": self.channel.value,
            "status": self.status.value,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
            "retry_count": self.retry_count,
            "error_details": self.error_details
        }


@dataclass
class NotificationConfig:
    """Configuration for a notification channel"""
    enabled: bool = False
    retry_count: int = 3
    retry_delay: int = 5


@dataclass
class EmailConfig(NotificationConfig):
    """Email channel configuration"""
    smtp_server: str = ""
    smtp_port: int = 587
    sender_email: str = ""
    sender_password: str = ""
    receiver_emails: List[str] = field(default_factory=list)
    use_tls: bool = True


@dataclass
class WeChatConfig(NotificationConfig):
    """WeChat bot configuration (Enterprise WeChat webhook)"""
    webhook_url: str = ""
    mentioned_list: List[str] = field(default_factory=list)
    mentioned_mobile_list: List[str] = field(default_factory=list)


@dataclass
class QQConfig(NotificationConfig):
    """QQ bot configuration (QQ robot webhook)"""
    webhook_url: str = ""
    group_id: Optional[str] = None


def retry_on_failure(max_retries: int = 3, delay: int = 5):
    """
    Decorator for retry mechanism on failure
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Delay between retries in seconds
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        logger.warning(
                            f"Attempt {attempt + 1}/{max_retries + 1} failed: {e}. "
                            f"Retrying in {delay} seconds..."
                        )
                        time.sleep(delay)
                    else:
                        logger.error(f"All {max_retries + 1} attempts failed")
            raise last_exception
        return wrapper
    return decorator


class BaseNotifier(ABC):
    """
    Abstract base class for notification channels
    All notification implementations must inherit from this class
    """
    
    def __init__(self, config: NotificationConfig):
        self.config = config
        self._status_file = LOGS_DIR / f"notification_status_{self.channel_type.value}.json"
    
    @property
    @abstractmethod
    def channel_type(self) -> NotificationChannel:
        """Return the channel type"""
        pass
    
    @abstractmethod
    def _send_message(self, content: str, subject: str = "") -> bool:
        """
        Internal method to send the notification
        Must be implemented by subclasses
        
        Args:
            content: The message content
            subject: Optional subject (for email)
        
        Returns:
            True if successful, False otherwise
        """
        pass
    
    def send(self, content: str, subject: str = "") -> NotificationResult:
        """
        Send notification with retry mechanism
        
        Args:
            content: The message content to send
            subject: Optional subject line
        
        Returns:
            NotificationResult with delivery status
        """
        if not self.config.enabled:
            return NotificationResult(
                channel=self.channel_type,
                status=NotificationStatus.FAILED,
                message="Channel is disabled",
                error_details="Notification channel is not enabled in configuration"
            )
        
        retry_count = 0
        last_error = None
        
        while retry_count <= self.config.retry_count:
            try:
                logger.info(f"Sending notification via {self.channel_type.value} (attempt {retry_count + 1})")
                
                success = self._send_message(content, subject)
                
                if success:
                    result = NotificationResult(
                        channel=self.channel_type,
                        status=NotificationStatus.SUCCESS,
                        message="Notification sent successfully",
                        retry_count=retry_count
                    )
                    self._log_status(result)
                    logger.info(f"Notification sent successfully via {self.channel_type.value}")
                    return result
                    
            except Exception as e:
                last_error = str(e)
                logger.error(f"Failed to send notification via {self.channel_type.value}: {e}")
            
            retry_count += 1
            if retry_count <= self.config.retry_count:
                logger.info(f"Retrying in {self.config.retry_delay} seconds...")
                time.sleep(self.config.retry_delay)
        
        result = NotificationResult(
            channel=self.channel_type,
            status=NotificationStatus.FAILED,
            message="Failed to send notification after all retries",
            retry_count=retry_count - 1,
            error_details=last_error
        )
        self._log_status(result)
        return result
    
    def _log_status(self, result: NotificationResult) -> None:
        """Log notification status to file"""
        try:
            status_data = []
            if self._status_file.exists():
                with open(self._status_file, 'r', encoding='utf-8') as f:
                    status_data = json.load(f)
            
            status_data.append(result.to_dict())
            
            status_data = status_data[-100:]
            
            with open(self._status_file, 'w', encoding='utf-8') as f:
                json.dump(status_data, f, indent=2, ensure_ascii=False, default=str)
                
        except Exception as e:
            logger.error(f"Failed to log status: {e}")


class EmailNotifier(BaseNotifier):
    """
    Email notification channel
    Supports SMTP with TLS/SSL
    """
    
    def __init__(self, config: EmailConfig):
        super().__init__(config)
        self.email_config = config
    
    @property
    def channel_type(self) -> NotificationChannel:
        return NotificationChannel.EMAIL
    
    def _send_message(self, content: str, subject: str = "") -> bool:
        """
        Send email via SMTP
        
        Args:
            content: Email body content (HTML supported)
            subject: Email subject line
        
        Returns:
            True if successful
        """
        if not self.email_config.receiver_emails:
            raise ValueError("No receiver emails configured")
        
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject or "Market Sentiment Report"
        msg['From'] = self.email_config.sender_email
        msg['To'] = ', '.join(self.email_config.receiver_emails)
        
        msg.attach(MIMEText(content, 'html', 'utf-8'))
        
        if self.email_config.use_tls:
            with smtplib.SMTP(
                self.email_config.smtp_server,
                self.email_config.smtp_port,
                timeout=30
            ) as server:
                server.starttls()
                server.login(
                    self.email_config.sender_email,
                    self.email_config.sender_password
                )
                server.sendmail(
                    self.email_config.sender_email,
                    self.email_config.receiver_emails,
                    msg.as_string()
                )
        else:
            with smtplib.SMTP_SSL(
                self.email_config.smtp_server,
                self.email_config.smtp_port,
                timeout=30
            ) as server:
                server.login(
                    self.email_config.sender_email,
                    self.email_config.sender_password
                )
                server.sendmail(
                    self.email_config.sender_email,
                    self.email_config.receiver_emails,
                    msg.as_string()
                )
        
        return True


class WeChatNotifier(BaseNotifier):
    """
    WeChat Work (Enterprise WeChat) bot notification channel
    Uses webhook URL for group notifications
    """
    
    def __init__(self, config: WeChatConfig):
        super().__init__(config)
        self.wechat_config = config
    
    @property
    def channel_type(self) -> NotificationChannel:
        return NotificationChannel.WECHAT
    
    def _send_message(self, content: str, subject: str = "") -> bool:
        """
        Send message to WeChat Work group via webhook
        
        Args:
            content: Message content (Markdown supported)
            subject: Optional subject prefix
        
        Returns:
            True if successful
        """
        import urllib.request
        import urllib.error
        
        if not self.wechat_config.webhook_url:
            raise ValueError("WeChat webhook URL not configured")
        
        full_content = f"**{subject}**\n\n{content}" if subject else content
        
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "content": full_content,
                "mentioned_list": self.wechat_config.mentioned_list,
                "mentioned_mobile_list": self.wechat_config.mentioned_mobile_list
            }
        }
        
        data = json.dumps(payload).encode('utf-8')
        
        request = urllib.request.Request(
            self.wechat_config.webhook_url,
            data=data,
            headers={
                'Content-Type': 'application/json',
                'User-Agent': 'MarketSentimentNotifier/1.0'
            },
            method='POST'
        )
        
        with urllib.request.urlopen(request, timeout=30) as response:
            result = json.loads(response.read().decode('utf-8'))
            if result.get('errcode', 0) != 0:
                raise Exception(f"WeChat API error: {result.get('errmsg', 'Unknown error')}")
        
        return True


class QQNotifier(BaseNotifier):
    """
    QQ bot notification channel
    Uses webhook URL for group/private message notifications
    Compatible with popular QQ bot frameworks (go-cqhttp, etc.)
    """
    
    def __init__(self, config: QQConfig):
        super().__init__(config)
        self.qq_config = config
    
    @property
    def channel_type(self) -> NotificationChannel:
        return NotificationChannel.QQ
    
    def _send_message(self, content: str, subject: str = "") -> bool:
        """
        Send message via QQ bot webhook
        
        Args:
            content: Message content
            subject: Optional subject prefix
        
        Returns:
            True if successful
        """
        import urllib.request
        import urllib.error
        
        if not self.qq_config.webhook_url:
            raise ValueError("QQ webhook URL not configured")
        
        full_content = f"[{subject}]\n{content}" if subject else content
        
        if self.qq_config.group_id:
            payload = {
                "group_id": self.qq_config.group_id,
                "message": full_content,
                "auto_escape": False
            }
        else:
            payload = {
                "message": full_content,
                "auto_escape": False
            }
        
        data = json.dumps(payload).encode('utf-8')
        
        request = urllib.request.Request(
            self.qq_config.webhook_url,
            data=data,
            headers={
                'Content-Type': 'application/json',
                'User-Agent': 'MarketSentimentNotifier/1.0'
            },
            method='POST'
        )
        
        with urllib.request.urlopen(request, timeout=30) as response:
            result = json.loads(response.read().decode('utf-8'))
            if result.get('status') == 'failed' or result.get('retcode', 0) != 0:
                raise Exception(f"QQ API error: {result.get('msg', 'Unknown error')}")
        
        return True


class NotificationManager:
    """
    Central notification manager
    Handles multiple channels and provides unified interface
    """
    
    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize notification manager
        
        Args:
            config_path: Path to configuration file (JSON format)
        """
        self.notifiers: Dict[NotificationChannel, BaseNotifier] = {}
        self._config_path = config_path or (Path(__file__).parent.parent / "config" / "notification_config.json")
        self._load_configuration()
    
    def _load_configuration(self) -> None:
        """Load configuration from JSON file"""
        if not self._config_path.exists():
            logger.warning(f"Configuration file not found: {self._config_path}")
            logger.info("Creating default configuration file...")
            self._create_default_config()
            return
        
        try:
            with open(self._config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            self._parse_configuration(config)
            logger.info("Configuration loaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise
    
    def _create_default_config(self) -> None:
        """Create default configuration file"""
        default_config = {
            "default_channel": "wechat",
            "retry_count": 3,
            "retry_delay": 5,
            "channels": {
                "email": {
                    "enabled": False,
                    "smtp_server": "smtp.example.com",
                    "smtp_port": 587,
                    "sender_email": "your_email@example.com",
                    "sender_password": "your_app_password",
                    "receiver_emails": ["receiver@example.com"],
                    "use_tls": True,
                    "retry_count": 3,
                    "retry_delay": 5
                },
                "wechat": {
                    "enabled": False,
                    "webhook_url": "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY",
                    "mentioned_list": [],
                    "mentioned_mobile_list": [],
                    "retry_count": 3,
                    "retry_delay": 5
                },
                "qq": {
                    "enabled": False,
                    "webhook_url": "http://localhost:5700/send_group_msg",
                    "group_id": None,
                    "retry_count": 3,
                    "retry_delay": 5
                }
            }
        }
        
        self._config_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(self._config_path, 'w', encoding='utf-8') as f:
            json.dump(default_config, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Default configuration created at: {self._config_path}")
    
    def _parse_configuration(self, config: Dict[str, Any]) -> None:
        """Parse configuration and initialize notifiers"""
        channels = config.get("channels", {})
        
        if "email" in channels:
            email_config = channels["email"]
            self.notifiers[NotificationChannel.EMAIL] = EmailNotifier(
                EmailConfig(
                    enabled=email_config.get("enabled", False),
                    smtp_server=email_config.get("smtp_server", ""),
                    smtp_port=email_config.get("smtp_port", 587),
                    sender_email=email_config.get("sender_email", ""),
                    sender_password=email_config.get("sender_password", ""),
                    receiver_emails=email_config.get("receiver_emails", []),
                    use_tls=email_config.get("use_tls", True),
                    retry_count=email_config.get("retry_count", 3),
                    retry_delay=email_config.get("retry_delay", 5)
                )
            )
        
        if "wechat" in channels:
            wechat_config = channels["wechat"]
            self.notifiers[NotificationChannel.WECHAT] = WeChatNotifier(
                WeChatConfig(
                    enabled=wechat_config.get("enabled", False),
                    webhook_url=wechat_config.get("webhook_url", ""),
                    mentioned_list=wechat_config.get("mentioned_list", []),
                    mentioned_mobile_list=wechat_config.get("mentioned_mobile_list", []),
                    retry_count=wechat_config.get("retry_count", 3),
                    retry_delay=wechat_config.get("retry_delay", 5)
                )
            )
        
        if "qq" in channels:
            qq_config = channels["qq"]
            self.notifiers[NotificationChannel.QQ] = QQNotifier(
                QQConfig(
                    enabled=qq_config.get("enabled", False),
                    webhook_url=qq_config.get("webhook_url", ""),
                    group_id=qq_config.get("group_id"),
                    retry_count=qq_config.get("retry_count", 3),
                    retry_delay=qq_config.get("retry_delay", 5)
                )
            )
        
        self._default_channel = config.get("default_channel", "wechat")
    
    def send_notification(
        self,
        content: str,
        subject: str = "",
        channels: Optional[List[NotificationChannel]] = None
    ) -> Dict[NotificationChannel, NotificationResult]:
        """
        Send notification through specified channels
        
        Args:
            content: Message content
            subject: Optional subject
            channels: List of channels to use (None = use default)
        
        Returns:
            Dict mapping channels to their results
        """
        if channels is None:
            default_type = NotificationChannel(self._default_channel)
            channels = [default_type] if default_type in self.notifiers else []
        
        results = {}
        
        for channel in channels:
            if channel in self.notifiers:
                notifier = self.notifiers[channel]
                results[channel] = notifier.send(content, subject)
            else:
                results[channel] = NotificationResult(
                    channel=channel,
                    status=NotificationStatus.FAILED,
                    message=f"Channel {channel.value} not configured",
                    error_details="Notifier not found in configuration"
                )
        
        return results
    
    def send_to_all(self, content: str, subject: str = "") -> Dict[NotificationChannel, NotificationResult]:
        """
        Send notification to all enabled channels
        
        Args:
            content: Message content
            subject: Optional subject
        
        Returns:
            Dict mapping channels to their results
        """
        enabled_channels = [
            channel for channel, notifier in self.notifiers.items()
            if notifier.config.enabled
        ]
        return self.send_notification(content, subject, enabled_channels)
    
    def get_enabled_channels(self) -> List[NotificationChannel]:
        """Get list of enabled channels"""
        return [
            channel for channel, notifier in self.notifiers.items()
            if notifier.config.enabled
        ]
    
    def is_any_channel_enabled(self) -> bool:
        """Check if any channel is enabled"""
        return any(notifier.config.enabled for notifier in self.notifiers.values())


class ReportFormatter:
    """
    Formats market sentiment reports for different notification channels
    """
    
    @staticmethod
    def format_for_email(report: str, date_str: str) -> str:
        """
        Convert Markdown report to HTML for email
        
        Args:
            report: Markdown formatted report
            date_str: Report date
        
        Returns:
            HTML formatted content
        """
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; max-width: 800px; margin: 0 auto; padding: 20px; }}
        h1 {{ color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
        h2 {{ color: #34495e; border-bottom: 1px solid #bdc3c7; padding-bottom: 5px; }}
        table {{ border-collapse: collapse; width: 100%; margin: 15px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #3498db; color: white; }}
        tr:nth-child(even) {{ background-color: #f2f2f2; }}
        code {{ background-color: #f4f4f4; padding: 2px 5px; border-radius: 3px; }}
        .positive {{ color: #27ae60; }}
        .negative {{ color: #e74c3c; }}
        .neutral {{ color: #f39c12; }}
    </style>
</head>
<body>
{ReportFormatter._markdown_to_html(report)}
</body>
</html>
        """
        return html_content
    
    @staticmethod
    def format_for_wechat(report: str, date_str: str) -> str:
        """
        Format report for WeChat (Markdown supported)
        
        Args:
            report: Original Markdown report
            date_str: Report date
        
        Returns:
            Formatted content for WeChat
        """
        lines = report.split('\n')
        formatted_lines = []
        
        for line in lines:
            if line.startswith('# '):
                formatted_lines.append(f"# {line[2:]}")
            elif line.startswith('## '):
                formatted_lines.append(f"\n## {line[3:]}")
            elif line.startswith('**') and line.endswith('**'):
                formatted_lines.append(f"\n> {line}")
            else:
                formatted_lines.append(line)
        
        return '\n'.join(formatted_lines)
    
    @staticmethod
    def format_for_qq(report: str, date_str: str) -> str:
        """
        Format report for QQ (plain text with basic formatting)
        
        Args:
            report: Original Markdown report
            date_str: Report date
        
        Returns:
            Plain text formatted content
        """
        lines = report.split('\n')
        formatted_lines = []
        
        for line in lines:
            if line.startswith('# '):
                formatted_lines.append(f"\n【{line[2:]}】")
            elif line.startswith('## '):
                formatted_lines.append(f"\n〖{line[3:]}〗")
            elif line.startswith('|'):
                cells = [c.strip() for c in line.split('|') if c.strip()]
                if not all(c.replace('-', '').replace(':', '') == '' for c in cells):
                    formatted_lines.append(' | '.join(cells))
            elif line.startswith('**') and line.endswith('**'):
                formatted_lines.append(f"● {line.strip('*')}")
            elif line.startswith('- '):
                formatted_lines.append(f"  • {line[2:]}")
            elif line.startswith('---'):
                formatted_lines.append('─' * 30)
            elif line.strip():
                formatted_lines.append(line.replace('**', ''))
        
        return '\n'.join(formatted_lines)
    
    @staticmethod
    def _markdown_to_html(markdown: str) -> str:
        """Simple Markdown to HTML conversion"""
        import re
        
        html = markdown
        
        html = re.sub(r'^### (.+)$', r'<h3>\1</h3>', html, flags=re.MULTILINE)
        html = re.sub(r'^## (.+)$', r'<h2>\1</h2>', html, flags=re.MULTILINE)
        html = re.sub(r'^# (.+)$', r'<h1>\1</h1>', html, flags=re.MULTILINE)
        
        html = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', html)
        
        html = re.sub(r'^- (.+)$', r'<li>\1</li>', html, flags=re.MULTILINE)
        
        def convert_table(match):
            lines = match.group(0).split('\n')
            rows = [l for l in lines if l.strip() and not re.match(r'^\|[\s\-:]+\|$', l)]
            
            if not rows:
                return ''
            
            table_html = '<table>\n'
            for i, row in enumerate(rows):
                cells = [c.strip() for c in row.split('|') if c.strip()]
                tag = 'th' if i == 0 else 'td'
                table_html += '<tr>' + ''.join(f'<{tag}>{c}</{tag}>' for c in cells) + '</tr>\n'
            table_html += '</table>'
            return table_html
        
        html = re.sub(r'(\|.+\|\n)+', convert_table, html)
        
        html = re.sub(r'```(.+?)```', r'<pre><code>\1</code></pre>', html, flags=re.DOTALL)
        
        html = re.sub(r'\n\n', '</p><p>', html)
        
        return f'<p>{html}</p>'


def send_daily_notification(
    report_content: str,
    report_date: str,
    config_path: Optional[Path] = None
) -> Dict[NotificationChannel, NotificationResult]:
    """
    Convenience function to send daily report notification
    
    Args:
        report_content: The full Markdown report content
        report_date: Report date string (YYYYMMDD)
        config_path: Optional path to configuration file
    
    Returns:
        Dict mapping channels to their results
    """
    manager = NotificationManager(config_path)
    
    if not manager.is_any_channel_enabled():
        logger.warning("No notification channels are enabled")
        return {}
    
    subject = f"Market Sentiment Report - {report_date}"
    
    results = {}
    
    for channel in manager.get_enabled_channels():
        if channel == NotificationChannel.EMAIL:
            formatted_content = ReportFormatter.format_for_email(report_content, report_date)
        elif channel == NotificationChannel.WECHAT:
            formatted_content = ReportFormatter.format_for_wechat(report_content, report_date)
        elif channel == NotificationChannel.QQ:
            formatted_content = ReportFormatter.format_for_qq(report_content, report_date)
        else:
            formatted_content = report_content
        
        channel_results = manager.send_notification(formatted_content, subject, [channel])
        results.update(channel_results)
    
    return results


if __name__ == "__main__":
    test_report = """
# Test Report

**Date**: 20240101

## Summary

| Metric | Value |
|--------|-------|
| Test 1 | Pass |
| Test 2 | Pass |

- Item 1
- Item 2
"""
    
    print("Testing Notification System...")
    print("=" * 50)
    
    manager = NotificationManager()
    
    print(f"\nEnabled channels: {[c.value for c in manager.get_enabled_channels()]}")
    
    if manager.is_any_channel_enabled():
        print("\nSending test notification...")
        results = manager.send_to_all("This is a test notification.", "Test Subject")
        
        for channel, result in results.items():
            print(f"\n{channel.value}: {result.status.value}")
            if result.error_details:
                print(f"  Error: {result.error_details}")
    else:
        print("\nNo channels enabled. Please configure notification_config.json")
