#!/usr/bin/env python3
"""
ALT End-to-End File Workflow Monitor

Features:
- Configurable time windows per step (IST source of truth + EST conversion in emails/logs).
- Monitors Step1 -> Step2 -> Step3 -> Step4 folders.
- Tracks only expected file patterns and extensions.
- Sends arrival, move-out, missing-file, stuck, and SLA alerts via SMTP port 25.
- Persists all state in SQL Server (SSMS) for restart recovery and idempotency.
- Handles startup backtrace (files already present before script starts).
- Retry logic for DB + SMTP operations.

Run:
    python monitor.py --config config.json
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import smtplib
import sys
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta
from email.mime.text import MIMEText
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pyodbc
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
EST = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


@dataclass
class MailConfig:
    smtp_host: str
    smtp_port: int
    from_addr: str
    internal_team: List[str]
    team1: List[str]
    timeout_sec: int = 15


@dataclass
class StepConfig:
    name: str
    folder: str
    expected_extension: str
    expected_count: int
    start_time_ist: str
    end_time_ist: str
    stuck_minutes: int
    polling_seconds: int
    require_prev_step_match: bool


@dataclass
class SLAConfig:
    default_sla_minutes: int
    large_file_threshold_mb: int
    large_file_estimated_minutes: int
    progress_mail_interval_minutes: int


@dataclass
class DBConfig:
    connection_string: str


@dataclass
class AppConfig:
    date_pattern: str
    filename_contains_date: bool
    db: DBConfig
    mail: MailConfig
    sla: SLAConfig
    steps: List[StepConfig]
    retry_count: int
    retry_delay_sec: int
    heartbeat_seconds: int


class GracefulKiller:
    def __init__(self) -> None:
        self.stop_event = threading.Event()
        signal.signal(signal.SIGINT, self._stop)
        signal.signal(signal.SIGTERM, self._stop)

    def _stop(self, *_: object) -> None:
        logging.warning("Shutdown signal received. Stopping monitor gracefully.")
        self.stop_event.set()


def parse_config(path: str) -> AppConfig:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    steps = [StepConfig(**s) for s in raw["steps"]]
    return AppConfig(
        date_pattern=raw["date_pattern"],
        filename_contains_date=raw.get("filename_contains_date", True),
        db=DBConfig(**raw["db"]),
        mail=MailConfig(**raw["mail"]),
        sla=SLAConfig(**raw["sla"]),
        steps=steps,
        retry_count=raw.get("retry_count", 3),
        retry_delay_sec=raw.get("retry_delay_sec", 5),
        heartbeat_seconds=raw.get("heartbeat_seconds", 30),
    )


class Retry:
    @staticmethod
    def run(fn, retries: int, delay: int, action: str):
        last_exc = None
        for i in range(1, retries + 1):
            try:
                return fn()
            except Exception as exc:  # intentional broad catch for resiliency
                last_exc = exc
                logging.exception("%s failed attempt %s/%s", action, i, retries)
                if i < retries:
                    time.sleep(delay)
        raise last_exc


class SqlStore:
    def __init__(self, config: AppConfig):
        self.config = config
        self.conn = None
        self.lock = threading.Lock()

    def connect(self) -> None:
        def _connect():
            self.conn = pyodbc.connect(self.config.db.connection_string, autocommit=False)

        Retry.run(_connect, self.config.retry_count, self.config.retry_delay_sec, "DB connect")
        self.ensure_schema()

    def ensure_schema(self) -> None:
        schema_sql = Path("schema.sql").read_text(encoding="utf-8")

        def _schema():
            with self.lock:
                cur = self.conn.cursor()
                cur.execute(schema_sql)
                self.conn.commit()

        Retry.run(_schema, self.config.retry_count, self.config.retry_delay_sec, "Schema setup")

    def upsert_file(
        self,
        step_name: str,
        file_name: str,
        full_path: str,
        size_bytes: int,
        arrived_at_utc: datetime,
        business_date: str,
    ) -> None:
        query = """
        MERGE alt_file_state AS target
        USING (SELECT ? step_name, ? file_name, ? business_date) src
        ON target.step_name=src.step_name AND target.file_name=src.file_name AND target.business_date=src.business_date
        WHEN MATCHED THEN
            UPDATE SET full_path=?, size_bytes=?, arrived_at_utc=COALESCE(target.arrived_at_utc, ?), updated_at_utc=SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT(step_name,file_name,business_date,full_path,size_bytes,arrived_at_utc,status,updated_at_utc)
            VALUES(?,?,?,?,?,?, 'ARRIVED', SYSUTCDATETIME());
        """

        def _run():
            with self.lock:
                cur = self.conn.cursor()
                cur.execute(
                    query,
                    step_name,
                    file_name,
                    business_date,
                    full_path,
                    size_bytes,
                    arrived_at_utc,
                    step_name,
                    file_name,
                    business_date,
                    full_path,
                    size_bytes,
                    arrived_at_utc,
                )
                self.conn.commit()

        Retry.run(_run, self.config.retry_count, self.config.retry_delay_sec, "upsert_file")

    def mark_moved(self, step_name: str, file_name: str, business_date: str, moved_at_utc: datetime) -> None:
        query = """
        UPDATE alt_file_state
        SET status='MOVED', moved_at_utc=?, updated_at_utc=SYSUTCDATETIME()
        WHERE step_name=? AND file_name=? AND business_date=?;
        """

        def _run():
            with self.lock:
                self.conn.cursor().execute(query, moved_at_utc, step_name, file_name, business_date)
                self.conn.commit()

        Retry.run(_run, self.config.retry_count, self.config.retry_delay_sec, "mark_moved")

    def mark_alert(self, step_name: str, file_name: str, business_date: str, alert_type: str) -> None:
        query = """
        IF NOT EXISTS(
            SELECT 1 FROM alt_alert_log WHERE step_name=? AND file_name=? AND business_date=? AND alert_type=?
        )
        INSERT INTO alt_alert_log(step_name,file_name,business_date,alert_type,created_at_utc)
        VALUES(?,?,?,?,SYSUTCDATETIME());
        """

        def _run():
            with self.lock:
                self.conn.cursor().execute(query, step_name, file_name, business_date, alert_type, step_name, file_name, business_date, alert_type)
                self.conn.commit()

        Retry.run(_run, self.config.retry_count, self.config.retry_delay_sec, "mark_alert")

    def alert_already_sent(self, step_name: str, file_name: str, business_date: str, alert_type: str) -> bool:
        query = "SELECT COUNT(1) FROM alt_alert_log WHERE step_name=? AND file_name=? AND business_date=? AND alert_type=?"

        def _run():
            with self.lock:
                cur = self.conn.cursor()
                cur.execute(query, step_name, file_name, business_date, alert_type)
                return cur.fetchone()[0] > 0

        return Retry.run(_run, self.config.retry_count, self.config.retry_delay_sec, "alert_already_sent")

    def get_expected_files_for_prev_step(self, prev_step: str, business_date: str) -> List[str]:
        query = "SELECT file_name FROM alt_file_state WHERE step_name=? AND business_date=?"

        def _run():
            with self.lock:
                cur = self.conn.cursor()
                cur.execute(query, prev_step, business_date)
                return [r[0] for r in cur.fetchall()]

        return Retry.run(_run, self.config.retry_count, self.config.retry_delay_sec, "get_expected_files_for_prev_step")

    def heartbeat(self, note: str) -> None:
        query = "INSERT INTO alt_heartbeat(note,created_at_utc) VALUES(?,SYSUTCDATETIME())"

        def _run():
            with self.lock:
                self.conn.cursor().execute(query, note)
                self.conn.commit()

        Retry.run(_run, self.config.retry_count, self.config.retry_delay_sec, "heartbeat")


class Mailer:
    def __init__(self, config: AppConfig):
        self.config = config

    def send(self, subject: str, body: str, recipients: List[str]) -> None:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = self.config.mail.from_addr
        msg["To"] = ",".join(recipients)

        def _send():
            with smtplib.SMTP(self.config.mail.smtp_host, self.config.mail.smtp_port, timeout=self.config.mail.timeout_sec) as smtp:
                smtp.sendmail(self.config.mail.from_addr, recipients, msg.as_string())

        Retry.run(_send, self.config.retry_count, self.config.retry_delay_sec, f"mail send: {subject}")


def now_ist() -> datetime:
    return datetime.now(tz=IST)


def parse_hhmm(raw: str) -> dt_time:
    hh, mm = raw.split(":")
    return dt_time(hour=int(hh), minute=int(mm))


def in_window(ts_ist: datetime, start_hhmm: str, end_hhmm: str) -> bool:
    start = datetime.combine(ts_ist.date(), parse_hhmm(start_hhmm), tzinfo=IST)
    end = datetime.combine(ts_ist.date(), parse_hhmm(end_hhmm), tzinfo=IST)
    return start <= ts_ist <= end


def date_in_name(file_name: str, pattern: str) -> bool:
    return pattern in file_name


def stable_file_size(path: Path, wait_sec: int = 2) -> Tuple[bool, int]:
    try:
        size1 = path.stat().st_size
        time.sleep(wait_sec)
        size2 = path.stat().st_size
        return size1 == size2, size2
    except FileNotFoundError:
        return False, 0


def to_est_str(ts_ist: datetime) -> str:
    return ts_ist.astimezone(EST).strftime("%Y-%m-%d %H:%M:%S %Z")


def to_ist_str(ts_ist: datetime) -> str:
    return ts_ist.strftime("%Y-%m-%d %H:%M:%S %Z")


class FileMonitor:
    def __init__(self, config: AppConfig, store: SqlStore, mailer: Mailer, killer: GracefulKiller):
        self.cfg = config
        self.store = store
        self.mailer = mailer
        self.killer = killer
        self.local_state: Dict[str, Dict[str, dict]] = {s.name: {} for s in config.steps}

    def run(self) -> None:
        logging.info("Starting monitor")
        self.backtrace_existing_files()
        heartbeat_ts = time.time()

        while not self.killer.stop_event.is_set():
            try:
                for step in self.cfg.steps:
                    self.scan_step(step)
                    self.enforce_step_rules(step)
                if time.time() - heartbeat_ts >= self.cfg.heartbeat_seconds:
                    self.store.heartbeat("alive")
                    heartbeat_ts = time.time()
                time.sleep(1)
            except Exception:
                logging.exception("Main loop error. Continuing (non-fatal).")
                time.sleep(2)

    def business_date(self, ts_ist: datetime) -> str:
        return ts_ist.strftime("%Y%m%d")

    def _matches_step_filters(self, step: StepConfig, file_path: Path, ts_ist: datetime) -> bool:
        if not file_path.is_file():
            return False
        if file_path.suffix.lower() != step.expected_extension.lower():
            return False
        if self.cfg.filename_contains_date and not date_in_name(file_path.name, self.business_date(ts_ist)):
            return False
        return True

    def backtrace_existing_files(self) -> None:
        ts_ist = now_ist()
        for step in self.cfg.steps:
            folder = Path(step.folder)
            if not folder.exists():
                logging.error("Missing folder at startup: %s", folder)
                continue
            for path in folder.iterdir():
                if not self._matches_step_filters(step, path, ts_ist):
                    continue
                stable, size = stable_file_size(path, wait_sec=1)
                if not stable:
                    continue
                ctime = datetime.fromtimestamp(path.stat().st_ctime, tz=IST)
                if in_window(ctime, step.start_time_ist, step.end_time_ist):
                    self.register_arrival(step, path, size, ctime, startup_backtrace=True)

    def scan_step(self, step: StepConfig) -> None:
        ts_ist = now_ist()
        folder = Path(step.folder)
        if not folder.exists():
            logging.error("Folder not found for %s: %s", step.name, folder)
            self.send_once(step, "_FOLDER", self.business_date(ts_ist), "FOLDER_MISSING", self.cfg.mail.internal_team,
                           f"{step.name} folder missing", f"Folder not found: {folder}")
            return

        current_names = set()
        for path in folder.iterdir():
            if not self._matches_step_filters(step, path, ts_ist):
                continue

            current_names.add(path.name)
            stable, size = stable_file_size(path)
            if not stable:
                logging.warning("Skipping unstable/partial file: %s", path)
                continue

            if path.name not in self.local_state[step.name]:
                arrival_ts = datetime.fromtimestamp(path.stat().st_ctime, tz=IST)
                if in_window(arrival_ts, step.start_time_ist, step.end_time_ist):
                    self.register_arrival(step, path, size, arrival_ts)
                else:
                    logging.info("Ignoring out-of-window file: %s (%s)", path.name, to_ist_str(arrival_ts))

        # detect moved out files
        for known_name, meta in list(self.local_state[step.name].items()):
            if known_name not in current_names and meta["status"] == "ARRIVED":
                move_ts_ist = now_ist()
                self.local_state[step.name][known_name]["status"] = "MOVED"
                self.local_state[step.name][known_name]["moved_at"] = move_ts_ist
                bdate = meta["business_date"]
                self.store.mark_moved(step.name, known_name, bdate, move_ts_ist.astimezone(UTC))
                self.mailer.send(
                    subject=f"[{step.name}] File moved out: {known_name}",
                    body=(
                        f"Hi Team,\n\nFile moved out from {step.name}.\n"
                        f"File: {known_name}\n"
                        f"Size: {meta['size_bytes']} bytes\n"
                        f"Moved(IST): {to_ist_str(move_ts_ist)}\n"
                        f"Moved(EST): {to_est_str(move_ts_ist)}\n"
                    ),
                    recipients=self.cfg.mail.team1,
                )

    def register_arrival(self, step: StepConfig, path: Path, size: int, arrival_ts_ist: datetime, startup_backtrace: bool = False) -> None:
        bdate = self.business_date(arrival_ts_ist)
        self.local_state[step.name][path.name] = {
            "status": "ARRIVED",
            "arrived_at": arrival_ts_ist,
            "size_bytes": size,
            "business_date": bdate,
            "next_progress_mail": arrival_ts_ist + timedelta(minutes=self.cfg.sla.progress_mail_interval_minutes),
        }

        self.store.upsert_file(
            step_name=step.name,
            file_name=path.name,
            full_path=str(path),
            size_bytes=size,
            arrived_at_utc=arrival_ts_ist.astimezone(UTC),
            business_date=bdate,
        )

        suffix = " (startup backtrace)" if startup_backtrace else ""
        self.mailer.send(
            subject=f"[{step.name}] File arrived: {path.name}{suffix}",
            body=(
                f"Hi Team, received file in {step.name}.\n\n"
                f"File: {path.name}\n"
                f"Size: {size} bytes\n"
                f"Arrival(IST): {to_ist_str(arrival_ts_ist)}\n"
                f"Arrival(EST): {to_est_str(arrival_ts_ist)}\n"
            ),
            recipients=self.cfg.mail.team1,
        )

    def enforce_step_rules(self, step: StepConfig) -> None:
        ts_ist = now_ist()
        bdate = self.business_date(ts_ist)

        # expected file count enforcement after end window
        if ts_ist > datetime.combine(ts_ist.date(), parse_hhmm(step.end_time_ist), tzinfo=IST):
            actual = len([m for m in self.local_state[step.name].values() if m["business_date"] == bdate])
            if actual < step.expected_count:
                self.send_once(
                    step,
                    "_WINDOW",
                    bdate,
                    "MISSING_FILES",
                    self.cfg.mail.internal_team,
                    f"[{step.name}] Missing files in window",
                    (
                        f"Expected {step.expected_count} file(s) in window {step.start_time_ist}-{step.end_time_ist} IST, "
                        f"but received {actual}."
                    ),
                )

        for file_name, meta in list(self.local_state[step.name].items()):
            if meta["status"] != "ARRIVED":
                continue
            arrived_at: datetime = meta["arrived_at"]
            size_mb = meta["size_bytes"] / (1024 * 1024)
            large = size_mb >= self.cfg.sla.large_file_threshold_mb
            elapsed = ts_ist - arrived_at

            # step sequencing check
            if step.require_prev_step_match:
                idx = [s.name for s in self.cfg.steps].index(step.name)
                prev_step = self.cfg.steps[idx - 1].name if idx > 0 else None
                if prev_step:
                    prev_files = self.store.get_expected_files_for_prev_step(prev_step, meta["business_date"])
                    base_prev = {Path(f).stem for f in prev_files}
                    if Path(file_name).stem not in base_prev:
                        self.send_once(
                            step,
                            file_name,
                            meta["business_date"],
                            "STEP_MISMATCH",
                            self.cfg.mail.internal_team,
                            f"[{step.name}] File mismatch with previous step",
                            f"File {file_name} did not exist in previous step {prev_step}.",
                        )

            # 5-min stuck alert (skip for large files)
            if not large and elapsed >= timedelta(minutes=step.stuck_minutes):
                self.send_once(
                    step,
                    file_name,
                    meta["business_date"],
                    "STUCK_5MIN",
                    self.cfg.mail.internal_team,
                    f"[{step.name}] Stuck alert (5 min): {file_name}",
                    f"Hi team, file appears stuck. File: {file_name}, Size: {meta['size_bytes']} bytes.",
                )

            # SLA / large-file progress
            if large:
                eta = arrived_at + timedelta(minutes=self.cfg.sla.large_file_estimated_minutes)
                if ts_ist >= meta["next_progress_mail"] and meta["status"] == "ARRIVED":
                    self.mailer.send(
                        subject=f"[{step.name}] Large file in progress: {file_name}",
                        body=(
                            f"Large file still in process.\n"
                            f"File: {file_name}\n"
                            f"SizeMB: {size_mb:.2f}\n"
                            f"ETA(IST): {to_ist_str(eta)}\n"
                            f"ETA(EST): {to_est_str(eta)}\n"
                        ),
                        recipients=self.cfg.mail.internal_team,
                    )
                    self.local_state[step.name][file_name]["next_progress_mail"] = ts_ist + timedelta(
                        minutes=self.cfg.sla.progress_mail_interval_minutes
                    )
                if ts_ist > eta:
                    self.send_once(
                        step,
                        file_name,
                        meta["business_date"],
                        "SLA_BREACH",
                        self.cfg.mail.internal_team,
                        f"[{step.name}] SLA breach: {file_name}",
                        (
                            f"File likely to breach SLA. Arrived(IST): {to_ist_str(arrived_at)}, "
                            f"expected completion(IST): {to_ist_str(eta)}"
                        ),
                    )
            else:
                # default SLA for normal files
                eta = arrived_at + timedelta(minutes=self.cfg.sla.default_sla_minutes)
                if ts_ist > eta:
                    self.send_once(
                        step,
                        file_name,
                        meta["business_date"],
                        "SLA_BREACH",
                        self.cfg.mail.internal_team,
                        f"[{step.name}] SLA breach: {file_name}",
                        (
                            f"File not moved within SLA. Arrived(IST): {to_ist_str(arrived_at)}, "
                            f"expected completion(IST): {to_ist_str(eta)}"
                        ),
                    )

    def send_once(
        self,
        step: StepConfig,
        file_name: str,
        bdate: str,
        alert_type: str,
        recipients: List[str],
        subject: str,
        body: str,
    ) -> None:
        if self.store.alert_already_sent(step.name, file_name, bdate, alert_type):
            return
        self.mailer.send(subject, body, recipients)
        self.store.mark_alert(step.name, file_name, bdate, alert_type)


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler("monitor.log", encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.json", help="Path to config json")
    args = parser.parse_args()

    setup_logging()
    cfg = parse_config(args.config)

    killer = GracefulKiller()
    store = SqlStore(cfg)
    store.connect()
    mailer = Mailer(cfg)
    monitor = FileMonitor(cfg, store, mailer, killer)
    monitor.run()


if __name__ == "__main__":
    main()
