"""Ingest Health Fitness Data_GoogleData/UserExercises_*.csv.

This is the driving activity set (see reconcile/activity_matcher.py) -- every row
is kept, including AUTO_DETECTED fragments where every tracker_* field is blank.
"""

import csv
import logging
import sqlite3
from pathlib import Path

from . import file_registry

logger = logging.getLogger(__name__)

SOURCE_GROUP = "user_exercises"

_COLUMNS = [
    "exercise_id", "exercise_start", "exercise_end", "utc_offset", "exercise_created",
    "exercise_last_updated", "activity_name", "log_type", "pool_length", "pool_length_unit",
    "intervals", "distance_units", "tracker_total_calories", "tracker_total_steps",
    "tracker_total_distance_mm", "tracker_total_altitude_mm", "tracker_avg_heart_rate",
    "tracker_peak_heart_rate", "tracker_avg_pace_mm_per_second", "tracker_avg_speed_mm_per_second",
    "tracker_peak_speed_mm_per_second", "tracker_auto_stride_run_mm", "tracker_auto_stride_walk_mm",
    "tracker_swim_lengths", "tracker_pool_length", "tracker_pool_length_unit", "tracker_cardio_load",
    "manually_logged_total_calories", "manually_logged_total_steps", "manually_logged_total_distance_mm",
    "manually_logged_pool_length", "manually_logged_pool_length_unit", "events",
]


def _to_float(v: str | None) -> float | None:
    if v is None or v == "":
        return None
    return float(v)


def _to_int(v: str | None) -> int | None:
    if v is None or v == "":
        return None
    return int(float(v))


def _normalize_ts(v: str) -> str:
    """'2024-08-03 19:24:04+0000' -> '2024-08-03T19:24:04Z' (assumes UTC offset, confirmed in real data)."""
    v = v.strip()
    if v.endswith("+0000"):
        v = v[: -len("+0000")]
    return v.replace(" ", "T") + "Z"


def ingest_file(conn: sqlite3.Connection, takeout_root: Path, csv_path: Path) -> int:
    relative_path = str(csv_path.relative_to(takeout_root))
    status = file_registry.check_file(conn, relative_path, csv_path)
    if not status.needs_ingest:
        return 0

    file_registry.begin_ingest(conn, relative_path, SOURCE_GROUP, csv_path, status.content_hash)
    file_registry.clear_prior_rows(conn, "raw_user_exercise", relative_path)

    row_count = 0
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = []
            for row in reader:
                rows.append((
                    int(row["exercise_id"]),
                    relative_path,
                    _normalize_ts(row["exercise_start"]),
                    _normalize_ts(row["exercise_end"]),
                    row.get("utc_offset") or None,
                    _normalize_ts(row["exercise_created"]) if row.get("exercise_created") else None,
                    _normalize_ts(row["exercise_last_updated"]) if row.get("exercise_last_updated") else None,
                    row["activity_name"],
                    row["log_type"],
                    _to_float(row.get("pool_length")),
                    row.get("pool_length_unit") or None,
                    row.get("intervals") or None,
                    row.get("distance_units") or None,
                    _to_float(row.get("tracker_total_calories")),
                    _to_int(row.get("tracker_total_steps")),
                    _to_int(row.get("tracker_total_distance_mm")),
                    _to_int(row.get("tracker_total_altitude_mm")),
                    _to_int(row.get("tracker_avg_heart_rate")),
                    _to_int(row.get("tracker_peak_heart_rate")),
                    _to_float(row.get("tracker_avg_pace_mm_per_second")),
                    _to_float(row.get("tracker_avg_speed_mm_per_second")),
                    _to_float(row.get("tracker_peak_speed_mm_per_second")),
                    _to_float(row.get("tracker_auto_stride_run_mm")),
                    _to_float(row.get("tracker_auto_stride_walk_mm")),
                    _to_int(row.get("tracker_swim_lengths")),
                    _to_float(row.get("tracker_pool_length")),
                    row.get("tracker_pool_length_unit") or None,
                    _to_float(row.get("tracker_cardio_load")),
                    _to_float(row.get("manually_logged_total_calories")),
                    _to_int(row.get("manually_logged_total_steps")),
                    _to_int(row.get("manually_logged_total_distance_mm")),
                    _to_float(row.get("manually_logged_pool_length")),
                    row.get("manually_logged_pool_length_unit") or None,
                    row.get("events") or None,
                ))
            conn.executemany(
                f"""INSERT INTO raw_user_exercise
                    (exercise_id, source_file, exercise_start_utc, exercise_end_utc, utc_offset,
                     exercise_created_utc, exercise_last_updated_utc, activity_name, log_type,
                     pool_length, pool_length_unit, intervals, distance_units,
                     tracker_total_calories, tracker_total_steps, tracker_total_distance_mm,
                     tracker_total_altitude_mm, tracker_avg_heart_rate, tracker_peak_heart_rate,
                     tracker_avg_pace_mm_per_second, tracker_avg_speed_mm_per_second,
                     tracker_peak_speed_mm_per_second, tracker_auto_stride_run_mm,
                     tracker_auto_stride_walk_mm, tracker_swim_lengths, tracker_pool_length,
                     tracker_pool_length_unit, tracker_cardio_load, manually_logged_total_calories,
                     manually_logged_total_steps, manually_logged_total_distance_mm,
                     manually_logged_pool_length, manually_logged_pool_length_unit, events)
                    VALUES ({",".join(["?"] * 34)})
                    ON CONFLICT(exercise_id) DO NOTHING""",
                rows,
            )
            row_count = len(rows)
        conn.commit()
        file_registry.finish_ingest_ok(conn, relative_path, row_count)
    except Exception as exc:
        conn.rollback()
        file_registry.finish_ingest_error(conn, relative_path, str(exc))
        logger.error("Failed to ingest %s: %s", csv_path, exc)
        raise
    return row_count


def ingest_all(conn: sqlite3.Connection, takeout_root: Path, health_fitness_dir: Path) -> int:
    total = 0
    for csv_path in sorted(health_fitness_dir.glob("UserExercises_*.csv")):
        total += ingest_file(conn, takeout_root, csv_path)
    return total
