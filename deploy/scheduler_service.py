#!/usr/bin/env python3
"""
Market Sentiment Report Scheduler Service
Executes daily at 20:00 Beijing Time (GMT+8) to generate Markdown reports.

Usage:
    python scheduler_service.py [--test] [--status] [--manual]

Options:
    --test       Run a single test execution immediately
    --status     Check the last execution status
    --manual     Trigger manual execution without waiting for schedule
"""

import argparse
import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any, Optional

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
            "next_scheduled": None
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
    """Handles the actual report generation"""
    
    def __init__(self, project_dir: Path):
        self.project_dir = project_dir
    
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
            "end_time": None
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
            result["end_time"] = datetime.now(BEIJING_TZ).isoformat()
            
            logger.info(f"Report generated successfully: {file_name}")
            
        except Exception as e:
            result["error"] = str(e)
            result["end_time"] = datetime.now(BEIJING_TZ).isoformat()
            logger.error(f"Report generation failed: {e}", exc_info=True)
        
        return result


class SchedulerService:
    """Main scheduler service that manages daily report generation"""
    
    def __init__(self):
        if not APSCHEDULER_AVAILABLE:
            raise ImportError(
                "APScheduler is not installed. "
                "Install it with: pip install apscheduler"
            )
        
        self.status = SchedulerStatus(STATUS_FILE)
        self.report_generator = ReportGenerator(PROJECT_DIR)
        self.scheduler = BackgroundScheduler(timezone=BEIJING_TZ)
        self.running = False
        
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
        logger.info("=" * 60)
        
        self.status.record_execution_start()
        
        result = self.report_generator.generate()
        
        if result["success"]:
            self.status.record_success(result["report_file"])
            logger.info(f"Report generation completed: {result['report_file']}")
        else:
            self.status.record_failure(result["error"])
            logger.error(f"Report generation failed: {result['error']}")
        
        self._update_next_scheduled()
        
        logger.info("=" * 60)
        logger.info("Scheduled report generation finished")
        logger.info("=" * 60)
    
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
    
    args = parser.parse_args()
    
    if not APSCHEDULER_AVAILABLE:
        print("Error: APScheduler is required. Install with: pip install apscheduler")
        sys.exit(1)
    
    if args.test or args.manual:
        service = SchedulerService()
        result = service.run_manual()
        
        if result.get('last_error') and not args.test:
            print(f"\nExecution completed with error: {result.get('last_error')}")
            sys.exit(1)
        else:
            if result.get('last_report_file'):
                print(f"\nExecution completed successfully!")
                print(f"Report file: {result.get('last_report_file')}")
            sys.exit(0)
    
    elif args.status:
        status_manager = SchedulerStatus(STATUS_FILE)
        status = status_manager.load()
        status["beijing_time"] = datetime.now(BEIJING_TZ).isoformat()
        print_status(status)
        sys.exit(0)
    
    else:
        service = SchedulerService()
        
        if args.daemon:
            service.run_forever()
        else:
            service.run_forever()


if __name__ == '__main__':
    main()
