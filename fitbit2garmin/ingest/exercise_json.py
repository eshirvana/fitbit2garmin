"""Ingest Global Export Data/exercise-*.json.

Each file is a JSON array of ~100 activity records in the classic Fitbit format.
Confirmed by direct cross-reference against UserExercises (see project plan):
startTime strings like "10/08/17 01:27:21" carry no offset marker but are UTC --
parse them as UTC directly, do not apply any local-timezone shift.
"""

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from . import file_registry

logger = logging.getLogger(__name__)

SOURCE_GROUP = "exercise_json"


def _parse_fitbit_datetime(v: str) -> str:
    """'MM/DD/YY HH:MM:SS' (confirmed UTC) -> ISO8601 'YYYY-MM-DDTHH:MM:SSZ'."""
    dt = datetime.strptime(v, "%m/%d/%y %H:%M:%S").replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def ingest_file(conn: sqlite3.Connection, takeout_root: Path, json_path: Path) -> int:
    relative_path = str(json_path.relative_to(takeout_root))
    status = file_registry.check_file(conn, relative_path, json_path)
    if not status.needs_ingest:
        return 0

    file_registry.begin_ingest(conn, relative_path, SOURCE_GROUP, json_path, status.content_hash)
    file_registry.clear_prior_rows(conn, "raw_exercise_json", relative_path)

    row_count = 0
    try:
        with open(json_path, encoding="utf-8") as f:
            records = json.load(f)

        rows = []
        for rec in records:
            log_id = rec.get("logId")
            if log_id is None:
                continue
            start_time = rec.get("startTime")
            if not start_time:
                continue
            source = rec.get("source") or {}
            rows.append((
                int(log_id),
                relative_path,
                _parse_fitbit_datetime(start_time),
                _parse_fitbit_datetime(rec["lastModified"]) if rec.get("lastModified") else None,
                rec.get("activityName"),
                rec.get("activityTypeId"),
                rec.get("duration"),
                rec.get("activeDuration"),
                rec.get("calories"),
                rec.get("steps"),
                rec.get("distance"),
                rec.get("distanceUnit"),
                rec.get("averageHeartRate"),
                rec.get("elevationGain"),
                1 if rec.get("hasGps") else 0,
                rec.get("logType"),
                source.get("name"),
                json.dumps(rec.get("activityLevel")) if rec.get("activityLevel") is not None else None,
                json.dumps(rec.get("heartRateZones")) if rec.get("heartRateZones") is not None else None,
            ))

        conn.executemany(
            """INSERT INTO raw_exercise_json
               (log_id, source_file, start_time_utc, last_modified_utc, activity_name,
                activity_type_id, duration_ms, active_duration_ms, calories, steps,
                distance, distance_unit, average_heart_rate, elevation_gain, has_gps,
                log_type, source_device, activity_level_json, heart_rate_zones_json)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
    for json_path in sorted(global_export_data_dir.glob("exercise-*.json")):
        total += ingest_file(conn, takeout_root, json_path)
    return total
