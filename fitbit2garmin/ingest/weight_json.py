"""Ingest Global Export Data/weight-*.json.

CONFIRMED against this user's real data (not assumed): the raw "weight" field is
in POUNDS, not kg, despite Fitbit's Web API nominally using metric internally --
Takeout apparently exports in the account's configured display unit. Verified by
cross-referencing Your Profile/Profile.csv's `weight_unit: en_US` (imperial) field
against the value range (141.3-210.5) and the profile's own reference weight
(94.3, itself in kg -> 207.9 lbs, matching the JSON log's upper range). Storing
canonically in kg here; output/csv_garmin_import.py applies locale conversion for
Garmin's importer at export time.
"""

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from . import file_registry

logger = logging.getLogger(__name__)

SOURCE_GROUP = "weight_json"

_LBS_TO_KG = 0.45359237


def _parse_entry_datetime(date_str: str, time_str: str) -> tuple[str, str]:
    """'04/19/16' + '23:59:59' -> ('2016-04-19', ISO8601 UTC).
    Fitbit's weight log date/time has no explicit offset; treated as the entry's
    local date (used for entry_date) with the time component passed through as if
    UTC for ordering purposes only -- Garmin's CSV import is date-only anyway
    (see output/csv_garmin_import.py), so no timezone precision is lost there."""
    dt = datetime.strptime(f"{date_str} {time_str}", "%m/%d/%y %H:%M:%S")
    entry_date = dt.strftime("%Y-%m-%d")
    entry_time_utc = dt.replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return entry_date, entry_time_utc


def ingest_file(conn: sqlite3.Connection, takeout_root: Path, json_path: Path) -> int:
    relative_path = str(json_path.relative_to(takeout_root))
    status = file_registry.check_file(conn, relative_path, json_path)
    if not status.needs_ingest:
        return 0

    file_registry.begin_ingest(conn, relative_path, SOURCE_GROUP, json_path, status.content_hash)
    file_registry.clear_prior_rows(conn, "weight_entry", relative_path)

    row_count = 0
    try:
        with open(json_path, encoding="utf-8") as f:
            records = json.load(f)

        rows = []
        for rec in records:
            log_id = rec.get("logId")
            weight_lbs = rec.get("weight")
            date_str, time_str = rec.get("date"), rec.get("time")
            if weight_lbs is None or not date_str or not time_str:
                continue
            entry_date, entry_time_utc = _parse_entry_datetime(date_str, time_str)
            rows.append((
                relative_path,
                log_id,
                entry_date,
                entry_time_utc,
                weight_lbs * _LBS_TO_KG,
                rec.get("bmi"),
                rec.get("fat"),
                rec.get("source"),
            ))

        conn.executemany(
            """INSERT INTO weight_entry
               (source_file, log_id, entry_date, entry_time_utc, weight_kg, bmi, body_fat_pct, source)
               VALUES (?,?,?,?,?,?,?,?)
               ON CONFLICT(log_id) DO NOTHING""",
            rows,
        )
        row_count = len(rows)
        conn.commit()
        file_registry.finish_ingest_ok(conn, relative_path, row_count)
    except Exception as exc:
        conn.rollback()
        file_registry.finish_ingest_error(conn, relative_path, str(exc))
        logger.error("Failed to ingest %s: %s", json_path, exc)
        raise
    return row_count


def ingest_all(conn: sqlite3.Connection, takeout_root: Path, global_export_data_dir: Path) -> int:
    total = 0
    for json_path in sorted(global_export_data_dir.glob("weight-*.json")):
        total += ingest_file(conn, takeout_root, json_path)
    return total
