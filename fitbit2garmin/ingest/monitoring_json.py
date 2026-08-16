"""Ingest Global Export Data's daily active-minutes JSON files (already
daily-granularity, confirmed by direct inspection -- unlike steps/calories/
distance in the same directory, which are per-minute) and sleep-*.json.
"""

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from . import file_registry

logger = logging.getLogger(__name__)


def _parse_fitbit_datetime(v: str) -> str:
    """'MM/DD/YY HH:MM:SS' -> ISO8601 UTC -- same confirmed-UTC convention as
    exercise-*.json (see ingest/exercise_json.py)."""
    dt = datetime.strptime(v, "%m/%d/%y %H:%M:%S").replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def ingest_daily_minutes(
    conn: sqlite3.Connection, takeout_root: Path, global_export_dir: Path,
    file_prefix: str, metric_type: str,
) -> int:
    source_group = f"monitoring_json:{metric_type}"
    total = 0
    for json_path in sorted(global_export_dir.glob(f"{file_prefix}-*.json")):
        relative_path = str(json_path.relative_to(takeout_root))
        status = file_registry.check_file(conn, relative_path, json_path)
        if not status.needs_ingest:
            continue

        file_registry.begin_ingest(conn, relative_path, source_group, json_path, status.content_hash)
        file_registry.clear_prior_rows(conn, "monitoring_metric", relative_path)

        try:
            with open(json_path, encoding="utf-8") as f:
                records = json.load(f)
            rows = [
                (metric_type, relative_path, _parse_fitbit_datetime(rec["dateTime"]), float(rec["value"]))
                for rec in records
            ]
            conn.executemany(
                "INSERT INTO monitoring_metric (metric_type, source_file, ts_utc, value) VALUES (?,?,?,?)",
                rows,
            )
            conn.commit()
            file_registry.finish_ingest_ok(conn, relative_path, len(rows))
            total += len(rows)
        except Exception as exc:
            conn.rollback()
            file_registry.finish_ingest_error(conn, relative_path, str(exc))
            logger.error("Failed to ingest %s: %s", json_path, exc)
            raise
    return total


def ingest_sleep(conn: sqlite3.Connection, takeout_root: Path, global_export_dir: Path) -> int:
    source_group = "monitoring_json:sleep"
    total = 0
    for json_path in sorted(global_export_dir.glob("sleep-*.json")):
        relative_path = str(json_path.relative_to(takeout_root))
        status = file_registry.check_file(conn, relative_path, json_path)
        if not status.needs_ingest:
            continue

        file_registry.begin_ingest(conn, relative_path, source_group, json_path, status.content_hash)
        file_registry.clear_prior_rows(conn, "sleep_entry", relative_path)

        try:
            with open(json_path, encoding="utf-8") as f:
                records = json.load(f)

            count = 0
            for rec in records:
                log_id = rec.get("logId")
                if log_id is None:
                    continue
                # startTime/endTime are naive 'YYYY-MM-DDTHH:MM:SS.mmm' with no
                # offset -- treated as UTC, consistent with exercise-*.json's
                # confirmed convention (not independently re-verified for sleep
                # specifically; lower stakes since sleep FIT is best-effort).
                start = rec.get("startTime", "").split(".")[0]
                end = rec.get("endTime", "").split(".")[0]
                cur = conn.execute(
                    """INSERT INTO sleep_entry
                       (source_file, log_id, start_time_utc, end_time_utc, duration_ms,
                        efficiency, minutes_asleep, minutes_awake, type)
                       VALUES (?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(log_id) DO NOTHING""",
                    (
                        relative_path, log_id,
                        f"{start}Z" if start else None, f"{end}Z" if end else None,
                        rec.get("duration"), rec.get("efficiency"),
                        rec.get("minutesAsleep"), rec.get("minutesAwake"), rec.get("type"),
                    ),
                )
                if cur.rowcount == 0:
                    continue
                sleep_entry_id = cur.lastrowid
                count += 1

                levels = rec.get("levels", {}) or {}
                for seg in levels.get("data", []) or []:
                    seg_start = (seg.get("dateTime") or "").split(".")[0]
                    seconds = seg.get("seconds")
                    level = seg.get("level")
                    if not seg_start or seconds is None or not level:
                        continue
                    conn.execute(
                        "INSERT INTO sleep_stage (sleep_entry_id, stage, start_time_utc, duration_s) VALUES (?,?,?,?)",
                        (sleep_entry_id, level, f"{seg_start}Z", int(seconds)),
                    )

            conn.commit()
            file_registry.finish_ingest_ok(conn, relative_path, count)
            total += count
        except Exception as exc:
            conn.rollback()
            file_registry.finish_ingest_error(conn, relative_path, str(exc))
            logger.error("Failed to ingest %s: %s", json_path, exc)
            raise
    return total


def ingest_all(conn: sqlite3.Connection, takeout_root: Path, global_export_dir: Path) -> dict:
    counts = {}
    counts["sedentary_minutes"] = ingest_daily_minutes(conn, takeout_root, global_export_dir, "sedentary_minutes", "sedentary_minutes")
    counts["lightly_active_minutes"] = ingest_daily_minutes(conn, takeout_root, global_export_dir, "lightly_active_minutes", "lightly_active_minutes")
    counts["moderately_active_minutes"] = ingest_daily_minutes(conn, takeout_root, global_export_dir, "moderately_active_minutes", "fairly_active_minutes")
    counts["very_active_minutes"] = ingest_daily_minutes(conn, takeout_root, global_export_dir, "very_active_minutes", "very_active_minutes")
    counts["sleep"] = ingest_sleep(conn, takeout_root, global_export_dir)
    return counts
