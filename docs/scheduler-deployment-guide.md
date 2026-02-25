# Market Sentiment Report Scheduler Service

## Overview

This scheduler service automatically generates market sentiment analysis reports daily at **20:00 Beijing Time (GMT+8)**. It uses APScheduler for reliable scheduling and integrates with systemd for service management.

## Features

- **Daily Execution**: Automatically runs at 20:00 Beijing Time
- **Markdown Reports**: Generates well-formatted `.md` files with timestamps
- **Comprehensive Logging**: Records all execution details, errors, and status
- **Error Recovery**: Automatic restart on failures with systemd
- **Manual Trigger**: Support for immediate test execution
- **Status Monitoring**: Built-in status checking mechanism

---

## Quick Start

### 1. Install Dependencies

```bash
cd /opt/NGAStockAnalyze
source venv/bin/activate
pip install -r requirements.txt
deactivate
```

### 2. Test Execution (Manual Run)

```bash
# Run a test execution immediately
cd /opt/NGAStockAnalyze
source venv/bin/activate
python deploy/scheduler_service.py --test
deactivate

# Or using deploy script
./deploy/deploy.sh manual-report
```

### 3. Install and Start Scheduler Service

```bash
# Install the scheduler service
./deploy/deploy.sh scheduler-install

# Start the scheduler
./deploy/deploy.sh scheduler-start

# Check status
./deploy/deploy.sh scheduler-status
```

---

## Service Management

### Using Deploy Script (Recommended)

| Command | Description |
|---------|-------------|
| `./deploy.sh scheduler-install` | Install scheduler as systemd service |
| `./deploy.sh scheduler-start` | Start the scheduler service |
| `./deploy.sh scheduler-stop` | Stop the scheduler service |
| `./deploy.sh scheduler-status` | Check service and execution status |
| `./deploy.sh scheduler-restart` | Restart the scheduler service |
| `./deploy.sh manual-report` | Trigger immediate report generation |

### Using Systemd Directly

```bash
# Start service
systemctl start market-sentiment-scheduler

# Stop service
systemctl stop market-sentiment-scheduler

# Check status
systemctl status market-sentiment-scheduler

# Enable auto-start on boot
systemctl enable market-sentiment-scheduler

# View logs
journalctl -u market-sentiment-scheduler -f
```

### Using Python Script Directly

```bash
# Start scheduler (foreground)
python deploy/scheduler_service.py --daemon

# Run manual test
python deploy/scheduler_service.py --manual

# Check status
python deploy/scheduler_service.py --status

# Run test and exit
python deploy/scheduler_service.py --test
```

---

## File Locations

| File | Path |
|------|------|
| Scheduler Script | `/opt/NGAStockAnalyze/deploy/scheduler_service.py` |
| Service File | `/etc/systemd/system/market-sentiment-scheduler.service` |
| Log File | `/opt/NGAStockAnalyze/logs/scheduler.log` |
| Error Log | `/opt/NGAStockAnalyze/logs/scheduler_error.log` |
| Status File | `/opt/NGAStockAnalyze/logs/scheduler_status.json` |
| Generated Reports | `/opt/NGAStockAnalyze/logs/YYYYMMDD_market_sentiment.md` |

---

## Testing and Verification

### 1. Manual Test Execution

```bash
# Option 1: Using Python script
python deploy/scheduler_service.py --test

# Option 2: Using deploy script
./deploy/deploy.sh manual-report
```

### 2. Verify Service is Running

```bash
# Check systemd status
systemctl status market-sentiment-scheduler

# Check execution status
python deploy/scheduler_service.py --status
```

### 3. Check Generated Files

```bash
# List generated reports
ls -la /opt/NGAStockAnalyze/logs/*.md

# View latest report
cat /opt/NGAStockAnalyze/logs/*_market_sentiment.md | head -50
```

### 4. Monitor Logs in Real-time

```bash
# System logs
journalctl -u market-sentiment-scheduler -f

# Application logs
tail -f /opt/NGAStockAnalyze/logs/scheduler.log
```

---

## Status Monitoring

The scheduler maintains a status file at `/opt/NGAStockAnalyze/logs/scheduler_status.json`:

```json
{
  "service_started": "2024-01-15T10:00:00+08:00",
  "last_execution": "2024-01-15T20:00:00+08:00",
  "last_success": "2024-01-15T20:00:05+08:00",
  "last_failure": null,
  "total_executions": 15,
  "total_successes": 15,
  "total_failures": 0,
  "last_error": null,
  "last_report_file": "20240115_market_sentiment.md",
  "next_scheduled": "2024-01-16T20:00:00+08:00"
}
```

### Status Fields

| Field | Description |
|-------|-------------|
| `service_started` | When the scheduler service was started |
| `last_execution` | Timestamp of the last execution attempt |
| `last_success` | Timestamp of the last successful execution |
| `last_failure` | Timestamp of the last failed execution |
| `total_executions` | Total number of execution attempts |
| `total_successes` | Total successful executions |
| `total_failures` | Total failed executions |
| `last_error` | Error message from the last failure |
| `last_report_file` | Filename of the last generated report |
| `next_scheduled` | Timestamp of the next scheduled run |

---

## Troubleshooting

### Common Issues

#### 1. Service Won't Start

```bash
# Check for errors in systemd
journalctl -xeu market-sentiment-scheduler

# Verify Python environment
source /opt/NGAStockAnalyze/venv/bin/activate
python -c "import apscheduler; print('OK')"

# Check file permissions
ls -la /opt/NGAStockAnalyze/deploy/scheduler_service.py
```

#### 2. Report Generation Fails

```bash
# Check error logs
tail -50 /opt/NGAStockAnalyze/logs/scheduler_error.log

# Run manual test to see detailed errors
python deploy/scheduler_service.py --manual

# Verify data sources
python -c "import akshare as ak; print(ak.stock_zt_pool_em(date='20240115'))"
```

#### 3. Service Runs But No Reports

```bash
# Check if scheduler is actually running
python deploy/scheduler_service.py --status

# Verify next scheduled time
systemctl status market-sentiment-scheduler

# Check timezone
timedatectl
```

#### 4. Timezone Issues

```bash
# Verify system timezone
timedatectl

# Set timezone to Asia/Shanghai if needed
sudo timedatectl set-timezone Asia/Shanghai

# Restart service after timezone change
systemctl restart market-sentiment-scheduler
```

### Debug Mode

For detailed debugging, run the scheduler manually in foreground:

```bash
# Stop systemd service first
systemctl stop market-sentiment-scheduler

# Run in foreground with verbose output
source /opt/NGAStockAnalyze/venv/bin/activate
python deploy/scheduler_service.py --daemon

# In another terminal, watch logs
tail -f /opt/NGAStockAnalyze/logs/scheduler.log
```

---

## Alternative: Using Systemd Timer (Optional)

If you prefer using systemd timers instead of APScheduler:

### 1. Install Timer Files

```bash
# Copy service and timer files
sudo cp deploy/market-sentiment-report.service /etc/systemd/system/
sudo cp deploy/market-sentiment-report.timer /etc/systemd/system/

# Reload systemd
sudo systemctl daemon-reload
```

### 2. Enable Timer

```bash
# Enable and start timer
sudo systemctl enable --now market-sentiment-report.timer

# Check timer status
systemctl list-timers market-sentiment-report.timer
```

### 3. Timer Management

```bash
# View timer info
systemctl list-timers

# Trigger immediately
systemctl start market-sentiment-report.service

# Check logs
journalctl -u market-sentiment-report.service
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Scheduler Service                         │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐    ┌─────────────────────────────┐    │
│  │   APScheduler   │───▶│  Report Generation Task     │    │
│  │  (20:00 BJ TZ)  │    │  - Fetch Data               │    │
│  └─────────────────┘    │  - Calculate Scores         │    │
│                         │  - Generate Markdown        │    │
│                         └─────────────────────────────┘    │
│                                      │                       │
│                                      ▼                       │
│  ┌─────────────────┐    ┌─────────────────────────────┐    │
│  │  Status Manager │◀───│  Logging & Error Handling   │    │
│  │  (JSON Status)  │    │  - Execution Logs           │    │
│  └─────────────────┘    │  - Error Tracking           │    │
│                         │  - Status Persistence       │    │
│                         └─────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────┐
│                     Systemd Service                          │
│  - Auto-restart on failure                                   │
│  - Boot-time activation                                      │
│  - Log management                                            │
└─────────────────────────────────────────────────────────────┘
```

---

## Best Practices

1. **Monitor Regularly**: Check status weekly with `--status` command
2. **Log Rotation**: Set up logrotate for `/opt/NGAStockAnalyze/logs/*.log`
3. **Backup Reports**: Archive generated reports periodically
4. **Alert Setup**: Configure alerts for consecutive failures
5. **Test After Updates**: Run manual test after code updates

### Example Logrotate Configuration

```bash
# /etc/logrotate.d/nga-stock-analyze
/opt/NGAStockAnalyze/logs/*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    create 644 root root
}
```
