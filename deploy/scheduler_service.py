#!/usr/bin/env python3
"""
Market Sentiment Report Scheduler Service
Executes daily at 20:00 Beijing Time (GMT+8) to generate Markdown reports.
Automatically sends notifications through configured channels.

Usage:
    python scheduler_service.py [--test] [--status] [--manual] [--no-notify]

Options:
    --test       Run a single test execution immediately
    --status     Check the last execution status
    --manual     Trigger manual execution without waiting for schedule
    --no-notify  Skip sending notifications
"""

import argparse
import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
    APSCHEDULER_AVAILABLE = True
except ImportError:
    APSCHEDULER_AVAILABLE = False

BEIJING_TZ = timezone(timedelta(hours=8))
SCHEDULE_HOUR = 20
SCHEDULE_MINUTE = 0

PROJECT_DIR = Path(__file__).parent.parent

from src.data_logger import LOGS_DIR, RESULTS_DIR, ARCHIVE_DIR, init_directories, FileArchiver

STATUS_FILE = LOGS_DIR / "scheduler_status.json"
SCHEDULER_LOG = LOGS_DIR / "scheduler.log"

init_directories()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(SCHEDULER_LOG, encoding='utf-8')
    ]
)
logger = logging.getLogger('SchedulerService')


class SchedulerStatus:
    """Manages scheduler execution status persistence"""
    
    def __init__(self, status_file: Path):
        self.status_file = status_file
        self.status_file.parent.mkdir(exist_ok=True)
    
    def load(self) -> Dict[str, Any]:
        if self.status_file.exists():
            try:
                with open(self.status_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Failed to load status file: {e}")
        return self._default_status()
    
    def save(self, status: Dict[str, Any]) -> None:
        try:
            with open(self.status_file, 'w', encoding='utf-8') as f:
                json.dump(status, f, indent=2, ensure_ascii=False, default=str)
        except IOError as e:
            logger.error(f"Failed to save status file: {e}")
    
    def _default_status(self) -> Dict[str, Any]:
        return {
            "service_started": None,
            "last_execution": None,
            "last_success": None,
            "last_failure": None,
            "total_executions": 0,
            "total_successes": 0,
            "total_failures": 0,
            "last_error": None,
            "last_report_file": None,
            "next_scheduled": None,
            "last_notification_status": None,
            "notification_enabled": True,
            "last_git_upload_status": None
        }
    
    def record_start(self) -> None:
        status = self.load()
        status["service_started"] = datetime.now(BEIJING_TZ).isoformat()
        self.save(status)
    
    def record_execution_start(self) -> None:
        status = self.load()
        status["last_execution"] = datetime.now(BEIJING_TZ).isoformat()
        status["total_executions"] += 1
        self.save(status)
    
    def record_success(self, report_file: str) -> None:
        status = self.load()
        now = datetime.now(BEIJING_TZ)
        status["last_success"] = now.isoformat()
        status["total_successes"] += 1
        status["last_report_file"] = report_file
        status["last_error"] = None
        self.save(status)
    
    def record_failure(self, error: str) -> None:
        status = self.load()
        now = datetime.now(BEIJING_TZ)
        status["last_failure"] = now.isoformat()
        status["total_failures"] += 1
        status["last_error"] = error
        self.save(status)
    
    def set_next_scheduled(self, next_time: datetime) -> None:
        status = self.load()
        status["next_scheduled"] = next_time.isoformat()
        self.save(status)


class ReportGenerator:
    """Handles the actual report generation, notification, and Git upload"""
    
    def __init__(self, project_dir: Path, enable_notification: bool = True):
        self.project_dir = project_dir
        self.enable_notification = enable_notification
        self._notification_manager = None
        self._git_upload_manager = None
    
    def _get_notification_manager(self):
        """
        Lazy load notification manager
        Returns None if notification is disabled or not configured
        """
        if not self.enable_notification:
            return None
        
        if self._notification_manager is None:
            try:
                from src.notifier import NotificationManager
                config_path = self.project_dir / "config" / "notification_config.json"
                self._notification_manager = NotificationManager(config_path)
            except Exception as e:
                logger.warning(f"Failed to initialize notification manager: {e}")
                return None
        
        return self._notification_manager
    
    def _get_git_upload_manager(self):
        """
        Lazy load Git upload manager
        Returns None if Git upload is disabled or not configured
        """
        if self._git_upload_manager is None:
            try:
                from src.git_uploader import GitUploadManager
                config_path = self.project_dir / "config" / "git_upload_config.json"
                self._git_upload_manager = GitUploadManager(config_path)
            except Exception as e:
                logger.warning(f"Failed to initialize Git upload manager: {e}")
                return None
        
        return self._git_upload_manager
    
    def generate(self) -> Dict[str, Any]:
        """
        Generate the market sentiment report.
        Returns dict with success status and report file path or error message.
        """
        result = {
            "success": False,
            "report_file": None,
            "error": None,
            "start_time": datetime.now(BEIJING_TZ).isoformat(),
            "end_time": None,
            "report_content": None,
            "notification_results": None,
            "git_upload_results": None,
            "analysis_date": None
        }
        
        try:
            from main import (
                get_trade_date, fetch_all_data, calculate_scores,
                analyze_sectors, predict_market_opening, generate_report,
                check_and_archive_folders
            )
            
            logger.info("Starting report generation...")
            
            date_str = get_trade_date()
            logger.info(f"Analysis date: {date_str}")
            result["analysis_date"] = date_str
            
            logger.info("Fetching all data...")
            data = fetch_all_data(date_str)
            
            logger.info("Calculating scores...")
            scores = calculate_scores(data)
            
            logger.info("Analyzing sectors...")
            sectors = analyze_sectors(data)
            
            logger.info("Predicting market opening...")
            opening_pred = predict_market_opening(scores, data)
            
            logger.info("Generating report...")
            report_str, file_name = generate_report(
                data, scores, sectors, date_str, opening_pred
            )
            
            logger.info("Checking folder sizes and archiving if needed...")
            check_and_archive_folders()
            
            result["success"] = True
            result["report_file"] = file_name
            result["report_content"] = report_str
            result["end_time"] = datetime.now(BEIJING_TZ).isoformat()
            
            logger.info(f"Report generated successfully: {file_name}")
            
            git_manager = self._get_git_upload_manager()
            if git_manager and git_manager.is_enabled():
                logger.info("Uploading report to Git repository...")
                try:
                    report_path = self.project_dir / "results" / file_name
                    git_success, git_message = git_manager.upload_report(report_path, date_str)
                    result["git_upload_results"] = {
                        "success": git_success,
                        "message": git_message
                    }
                    if git_success:
                        logger.info(f"Git upload successful: {git_message}")
                    else:
                        logger.warning(f"Git upload failed: {git_message}")
                except Exception as e:
                    logger.error(f"Failed to upload to Git: {e}")
                    result["git_upload_results"] = {
                        "success": False,
                        "error": str(e)
                    }
            else:
                logger.info("Git upload disabled or not configured")
                result["git_upload_results"] = {"status": "disabled"}
            
            notification_manager = self._get_notification_manager()
            if notification_manager and notification_manager.is_any_channel_enabled():
                logger.info("Sending notifications...")
                try:
                    from src.notifier import send_daily_notification
                    notification_results = send_daily_notification(
                        report_content=report_str,
                        report_date=date_str,
                        config_path=self.project_dir / "config" / "notification_config.json"
                    )
                    result["notification_results"] = {
                        channel.value: {
                            "status": r.status.value,
                            "message": r.message,
                            "error": r.error_details
                        }
                        for channel, r in notification_results.items()
                    }
                    
                    success_count = sum(1 for r in notification_results.values() if r.status.value == "success")
                    total_count = len(notification_results)
                    logger.info(f"Notifications sent: {success_count}/{total_count} successful")
                    
                except Exception as e:
                    logger.error(f"Failed to send notifications: {e}")
                    result["notification_results"] = {"error": str(e)}
            else:
                logger.info("Notifications disabled or no channels configured")
                result["notification_results"] = {"status": "disabled"}
            
        except Exception as e:
            result["error"] = str(e)
            result["end_time"] = datetime.now(BEIJING_TZ).isoformat()
            logger.error(f"Report generation failed: {e}", exc_info=True)
        
        return result


class SchedulerService:
    """Main scheduler service that manages daily report generation"""
    
    def __init__(self, enable_notification: bool = True):
        if not APSCHEDULER_AVAILABLE:
            raise ImportError(
                "APScheduler is not installed. "
                "Install it with: pip install apscheduler"
            )
        
        self.status = SchedulerStatus(STATUS_FILE)
        self.report_generator = ReportGenerator(PROJECT_DIR, enable_notification)
        self.scheduler = BackgroundScheduler(timezone=BEIJING_TZ)
        self.running = False
        self.enable_notification = enable_notification
        
        self.scheduler.add_listener(
            self._job_executed_listener,
            EVENT_JOB_EXECUTED | EVENT_JOB_ERROR
        )
    
    def _job_executed_listener(self, event) -> None:
        if event.exception:
            logger.error(f"Scheduled job failed with exception: {event.exception}")
        else:
            logger.info("Scheduled job completed")
    
    def execute_report_generation(self) -> None:
        """Execute the report generation task"""
        logger.info("=" * 60)
        logger.info("Scheduled report generation started")
        logger.info(f"Beijing Time: {datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Notifications: {'Enabled' if self.enable_notification else 'Disabled'}")
        logger.info("=" * 60)
        
        self.status.record_execution_start()
        
        result = self.report_generator.generate()
        
        if result["success"]:
            self.status.record_success(result["report_file"])
            logger.info(f"Report generation completed: {result['report_file']}")
            
            if result.get("git_upload_results"):
                self._record_git_upload_status(result["git_upload_results"])
            
            if result.get("notification_results"):
                self._record_notification_status(result["notification_results"])
        else:
            self.status.record_failure(result["error"])
            logger.error(f"Report generation failed: {result['error']}")
        
        self._update_next_scheduled()
        
        logger.info("=" * 60)
        logger.info("Scheduled report generation finished")
        logger.info("=" * 60)
    
    def _record_git_upload_status(self, git_upload_results: Dict[str, Any]) -> None:
        """Record Git upload status"""
        try:
            status = self.status.load()
            status["last_git_upload_status"] = git_upload_results
            self.status.save(status)
            logger.info(f"Git upload status recorded: {git_upload_results}")
        except Exception as e:
            logger.error(f"Failed to record Git upload status: {e}")
    
    def _record_notification_status(self, notification_results: Dict[str, Any]) -> None:
        """Record notification delivery status"""
        try:
            status = self.status.load()
            status["last_notification_status"] = notification_results
            self.status.save(status)
            logger.info(f"Notification status recorded: {notification_results}")
        except Exception as e:
            logger.error(f"Failed to record notification status: {e}")
    
    def _update_next_scheduled(self) -> None:
        jobs = self.scheduler.get_jobs()
        for job in jobs:
            if job.id == 'daily_report':
                next_run = job.next_run_time
                if next_run:
                    self.status.set_next_scheduled(next_run.astimezone(BEIJING_TZ))
                break
    
    def start(self) -> None:
        """Start the scheduler service"""
        logger.info("Starting Scheduler Service...")
        
        self.status.record_start()
        
        trigger = CronTrigger(
            hour=SCHEDULE_HOUR,
            minute=SCHEDULE_MINUTE,
            timezone=BEIJING_TZ
        )
        
        self.scheduler.add_job(
            self.execute_report_generation,
            trigger=trigger,
            id='daily_report',
            name='Daily Market Sentiment Report',
            misfire_grace_time=3600,
            coalesce=True
        )
        
        self.scheduler.start()
        self.running = True
        
        self._update_next_scheduled()
        
        jobs = self.scheduler.get_jobs()
        for job in jobs:
            logger.info(f"Scheduled job: {job.name}")
            logger.info(f"  Next run: {job.next_run_time.astimezone(BEIJING_TZ) if job.next_run_time else 'N/A'}")
        
        logger.info(f"Scheduler started. Daily execution at {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d} Beijing Time")
        
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def stop(self) -> None:
        """Stop the scheduler service"""
        if self.running:
            logger.info("Stopping Scheduler Service...")
            self.scheduler.shutdown(wait=True)
            self.running = False
            logger.info("Scheduler Service stopped")
    
    def _signal_handler(self, signum, frame) -> None:
        logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
        sys.exit(0)
    
    def run_forever(self) -> None:
        """Start the scheduler and run until interrupted"""
        self.start()
        
        try:
            import time
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
        finally:
            self.stop()
    
    def get_status(self) -> Dict[str, Any]:
        """Get current scheduler status"""
        status = self.status.load()
        
        jobs = self.scheduler.get_jobs()
        for job in jobs:
            if job.id == 'daily_report':
                status["job_name"] = job.name
                status["next_run"] = (
                    job.next_run_time.astimezone(BEIJING_TZ).isoformat()
                    if job.next_run_time else None
                )
                break
        
        status["scheduler_running"] = self.running
        status["beijing_time"] = datetime.now(BEIJING_TZ).isoformat()
        
        return status
    
    def run_manual(self) -> Dict[str, Any]:
        """Run a manual execution immediately"""
        logger.info("Manual execution triggered")
        self.execute_report_generation()
        return self.get_status()


def print_status(status: Dict[str, Any]) -> None:
    """Pretty print the scheduler status"""
    print("\n" + "=" * 60)
    print("SCHEDULER STATUS")
    print("=" * 60)
    
    print(f"\nBeijing Time: {status.get('beijing_time', 'N/A')}")
    print(f"Service Started: {status.get('service_started', 'N/A')}")
    print(f"Scheduler Running: {status.get('scheduler_running', False)}")
    
    print("\n--- Folder Sizes ---")
    archiver = FileArchiver()
    logs_size = archiver.get_folder_size_mb(LOGS_DIR)
    results_size = archiver.get_folder_size_mb(RESULTS_DIR)
    archives_size = archiver.get_folder_size_mb(ARCHIVE_DIR) if ARCHIVE_DIR.exists() else 0
    print(f"Logs Folder: {logs_size:.2f} MB (threshold: {50} MB)")
    print(f"Results Folder: {results_size:.2f} MB (threshold: {50} MB)")
    print(f"Archives Folder: {archives_size:.2f} MB")
    
    print("\n--- Execution Statistics ---")
    print(f"Total Executions: {status.get('total_executions', 0)}")
    print(f"Total Successes: {status.get('total_successes', 0)}")
    print(f"Total Failures: {status.get('total_failures', 0)}")
    
    print("\n--- Last Execution ---")
    print(f"Last Execution: {status.get('last_execution', 'N/A')}")
    print(f"Last Success: {status.get('last_success', 'N/A')}")
    print(f"Last Failure: {status.get('last_failure', 'N/A')}")
    
    if status.get('last_report_file'):
        print(f"Last Report File: {status.get('last_report_file')}")
    
    if status.get('last_error'):
        print(f"Last Error: {status.get('last_error')}")
    
    print("\n--- Schedule ---")
    print(f"Next Scheduled Run: {status.get('next_scheduled', 'N/A')}")
    print(f"Schedule: Daily at {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d} Beijing Time (GMT+8)")
    
    if status.get('last_git_upload_status'):
        print("\n--- Last Git Upload Status ---")
        git_status = status.get('last_git_upload_status')
        if isinstance(git_status, dict):
            print(f"  Success: {git_status.get('success', 'N/A')}")
            if git_status.get('message'):
                print(f"  Message: {git_status.get('message')}")
            if git_status.get('error'):
                print(f"  Error: {git_status.get('error')}")
    
    if status.get('last_notification_status'):
        print("\n--- Last Notification Status ---")
        notification_status = status.get('last_notification_status')
        if isinstance(notification_status, dict):
            for channel, details in notification_status.items():
                if isinstance(details, dict):
                    print(f"  {channel}: {details.get('status', 'N/A')}")
                    if details.get('error'):
                        print(f"    Error: {details.get('error')}")
                else:
                    print(f"  {channel}: {details}")
    
    print("\n" + "=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description='Market Sentiment Report Scheduler Service',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python scheduler_service.py              Start the scheduler service
    python scheduler_service.py --test       Run a test execution
    python scheduler_service.py --manual     Trigger manual execution
    python scheduler_service.py --status     Check execution status
    python scheduler_service.py --no-notify  Run without notifications
        """
    )
    
    parser.add_argument(
        '--test',
        action='store_true',
        help='Run a single test execution and exit'
    )
    parser.add_argument(
        '--manual',
        action='store_true',
        help='Trigger immediate manual execution'
    )
    parser.add_argument(
        '--status',
        action='store_true',
        help='Check the last execution status and exit'
    )
    parser.add_argument(
        '--daemon',
        action='store_true',
        help='Run as daemon (foreground, for systemd)'
    )
    parser.add_argument(
        '--no-notify',
        action='store_true',
        help='Disable notification sending'
    )
    
    args = parser.parse_args()
    
    if not APSCHEDULER_AVAILABLE:
        print("Error: APScheduler is required. Install with: pip install apscheduler")
        sys.exit(1)
    
    enable_notification = not args.no_notify
    
    if args.test or args.manual:
        service = SchedulerService(enable_notification=enable_notification)
        result = service.run_manual()
        
        if result.get('last_error') and not args.test:
            print(f"\nExecution completed with error: {result.get('last_error')}")
            sys.exit(1)
        else:
            if result.get('last_report_file'):
                print(f"\nExecution completed successfully!")
                print(f"Report file: {result.get('last_report_file')}")
                
                if result.get('last_notification_status'):
                    print(f"\nNotification Status:")
                    for channel, details in result.get('last_notification_status', {}).items():
                        print(f"  {channel}: {details}")
            sys.exit(0)
    
    elif args.status:
        status_manager = SchedulerStatus(STATUS_FILE)
        status = status_manager.load()
        status["beijing_time"] = datetime.now(BEIJING_TZ).isoformat()
        print_status(status)
        sys.exit(0)
    
    else:
        service = SchedulerService(enable_notification=enable_notification)
        
        if args.daemon:
            service.run_forever()
        else:
            service.run_forever()


if __name__ == '__main__':
    main()
