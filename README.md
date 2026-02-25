# ALT File Monitoring Automation (Python + SQL Server + SMTP 25)

This solution automates your manual workflow end-to-end for Step1 -> Step2 -> Step3 -> Step4.

## What it does
- Monitors folders for expected files within each step's IST time window (`start_time_ist`, `end_time_ist`).
- Converts and includes EST timestamps in email content.
- Sends arrival and move-out mails to Team1.
- Sends missing-file, stuck, SLA breach, mismatch, and folder-missing alerts to Internal Team.
- Skips 5-minute stuck alert for large files and sends 30-minute progress mails.
- Persists state in SQL Server for restart recovery and idempotent alerting.
- Backtraces startup files (if file already exists when script starts, it is included only if arrival time falls inside time window).

## Files
- `monitor.py` - main production script.
- `config.json` - all configurable settings (time windows, folders, SLA, SMTP, SQL).
- `schema.sql` - SQL Server tables for file state, alerts, heartbeat.

## Prerequisites
```bash
pip install pyodbc
```
Install SQL Server ODBC driver (17 or 18) on your machine.

## SQL setup
1. Create a database in SSMS.
2. Update `db.connection_string` in `config.json`.
3. `monitor.py` auto-runs `schema.sql` at startup.

## Run
```bash
python monitor.py --config config.json
```

## Important configuration points
Update these values in `config.json`:
- SMTP host and SQL Server connection string.
- `date_pattern` (e.g., `20260225`) if you want fixed-date testing.
- Per-step `start_time_ist` and `end_time_ist` (each step has independent start/end and can be changed later).
- Per-step folders and extensions.

## EST conversion reference
For normal days:
- **06:00 IST ≈ 19:30 EST (previous day)**
- **08:30 IST ≈ 22:00 EST (previous day)**

During Daylight Saving Time, EST becomes EDT; script handles this automatically using timezone database.

## Notes on resiliency
- Retries for DB and SMTP operations.
- Try/except around main loop so one failure doesn't stop the service.
- SQL alert log avoids duplicate emails.
- Heartbeat row written periodically to confirm liveness.
- Non-blocking loop with small sleeps.

## Recommended production usage
- Run from Windows Task Scheduler (start before 06:00 IST).
- Keep process running continuously for monitoring day.
- Archive or rotate `monitor.log` daily.
