"""Ingest lower-priority daily/intraday CSVs from Physical Activity_GoogleData.

Scope decision (Phase 3, best-effort): only DAILY-granularity values are stored.
steps/calories/distance/floors source files are minute-level (confirmed by direct
inspection) and are aggregated to one daily sum here rather than stored raw --
storing every minute-level reading would reproduce the exact 19M-row memory
problem the old codebase's HR handling was specifically designed to avoid (see
CLAUDE.md), for data that's explicitly lowest-priority/best-effort in this
project. daily_resting_heart_rate / daily_oxygen_saturation / daily_heart_rate_
variability are already daily in the source and are passed through directly.
"""

import csv
import logging
import sqlite3
from collections import defaultdict
from pathlib import Path

from . import file_registry

logger = logging.getLogger(__name__)

SOURCE_GROUP_PREFIX = "monitoring_csv"


def ingest_daily_sum(
    conn: sqlite3.Connection, takeout_root: Path, dir_path: Path,
    file_prefix: str, metric_type: str, value_column: str,
) -> int:
    """Sum a minute-level metric (steps/calories/distance/floors) to one row per
    UTC calendar day. Each source file is re-aggregated in full on (re)ingest --
    cheap since these are daily totals, not a per-reading table."""
    source_group = f"{SOURCE_GROUP_PREFIX}:{metric_type}"
    total_days = 0
    for csv_path in sorted(dir_path.glob(f"{file_prefix}_*.csv")):
        relative_path = str(csv_path.relative_to(takeout_root))
        status = file_registry.check_file(conn, relative_path, csv_path)
        if not status.needs_ingest:
            continue

        file_registry.begin_ingest(conn, relative_path, source_group, csv_path, status.content_hash)
        file_registry.clear_prior_rows(conn, "monitoring_metric", relative_path)

        try:
            daily_sums: dict[str, float] = defaultdict(float)
            with open(csv_path, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    day = row["timestamp"][:10]  # 'YYYY-MM-DD' prefix of the ISO timestamp
                    v = row.get(value_column)
                    if v not in (None, ""):
                        daily_sums[day] += float(v)

            rows = [
                (metric_type, relative_path, f"{day}T00:00:00Z", total, "sum")
                for day, total in daily_sums.items()
            ]
            conn.executemany(
                "INSERT INTO monitoring_metric (metric_type, source_file, ts_utc, value, unit) VALUES (?,?,?,?,?)",
                rows,
            )
            conn.commit()
            file_registry.finish_ingest_ok(conn, relative_path, len(rows))
            total_days += len(rows)
        except Exception as exc:
            conn.rollback()
            file_registry.finish_ingest_error(conn, relative_path, str(exc))
            logger.error("Failed to ingest %s: %s", csv_path, exc)
            raise
    return total_days


def _ingest_daily_direct_file(
    conn: sqlite3.Connection, relative_path: str, csv_path: Path,
    metric_type: str, value_column: str, value_column2: str | None,
) -> int:
    source_group = f"{SOURCE_GROUP_PREFIX}:{metric_type}"
    status = file_registry.check_file(conn, relative_path, csv_path)
    if not status.needs_ingest:
        return 0

    file_registry.begin_ingest(conn, relative_path, source_group, csv_path, status.content_hash)
    file_registry.clear_prior_rows(conn, "monitoring_metric", relative_path)

    row_count = 0
    try:
        rows = []
        with open(csv_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                v = row.get(value_column)
                if v in (None, ""):
                    continue
                v2 = row.get(value_column2) if value_column2 else None
                rows.append((metric_type, relative_path, row["timestamp"], float(v), float(v2) if v2 else None, None))
        conn.executemany(
            "INSERT INTO monitoring_metric (metric_type, source_file, ts_utc, value, value2, unit) VALUES (?,?,?,?,?,?)",
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


def ingest_daily_direct(
    conn: sqlite3.Connection, takeout_root: Path, dir_path: Path,
    filename: str, metric_type: str, value_column: str, value_column2: str | None = None,
) -> int:
    """Passthrough for already-daily CSVs that are a SINGLE file (confirmed by
    direct inspection: daily_resting_heart_rate.csv, daily_heart_rate_variability.csv
    are not date-sharded, unlike most other Physical Activity_GoogleData files)."""
    csv_path = dir_path / filename
    if not csv_path.exists():
        return 0
    return _ingest_daily_direct_file(
        conn, str(csv_path.relative_to(takeout_root)), csv_path, metric_type, value_column, value_column2
    )


def ingest_daily_direct_sharded(
    conn: sqlite3.Connection, takeout_root: Path, dir_path: Path,
    file_prefix: str, metric_type: str, value_column: str, value_column2: str | None = None,
) -> int:
    """Passthrough for already-daily CSVs that ARE date-sharded (confirmed:
    daily_oxygen_saturation_YYYY-MM-DD.csv, 23 files -- unlike the two single-file
    daily metrics above)."""
    total = 0
    for csv_path in sorted(dir_path.glob(f"{file_prefix}_*.csv")):
        total += _ingest_daily_direct_file(
            conn, str(csv_path.relative_to(takeout_root)), csv_path, metric_type, value_column, value_column2
        )
    return total


def ingest_all(conn: sqlite3.Connection, takeout_root: Path, physical_activity_dir: Path) -> dict:
    counts = {}
    counts["steps_daily"] = ingest_daily_sum(conn, takeout_root, physical_activity_dir, "steps", "steps_daily", "steps")
    counts["calories_daily"] = ingest_daily_sum(conn, takeout_root, physical_activity_dir, "calories", "calories_daily", "calories")
    counts["distance_daily"] = ingest_daily_sum(conn, takeout_root, physical_activity_dir, "distance", "distance_daily", "distance")
    counts["floors_daily"] = ingest_daily_sum(conn, takeout_root, physical_activity_dir, "floors", "floors_daily", "floors")
    counts["resting_heart_rate"] = ingest_daily_direct(
        conn, takeout_root, physical_activity_dir, "daily_resting_heart_rate.csv",
        "resting_heart_rate", "beats per minute",
    )
    counts["spo2_daily"] = ingest_daily_direct_sharded(
        conn, takeout_root, physical_activity_dir, "daily_oxygen_saturation",
        "spo2_daily", "average percentage",
    )
    counts["hrv_daily"] = ingest_daily_direct(
        conn, takeout_root, physical_activity_dir, "daily_heart_rate_variability.csv",
        "hrv_daily", "average heart rate variability milliseconds",
    )
    return counts
