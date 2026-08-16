"""Ingest Physical Activity_GoogleData/gps_location_*.csv.

Continuous per-day location log, NOT scoped to a specific activity. source_key is
the day the file covers ('YYYY-MM-DD', from the filename), used by
reconcile/gps_attacher.py to slice out points inside a given activity's time window
as a fallback when no matching TCX file exists.
"""

import csv
import logging
import sqlite3
from pathlib import Path

from . import file_registry

logger = logging.getLogger(__name__)

SOURCE_GROUP = "gps_location_csv"


def ingest_file(conn: sqlite3.Connection, takeout_root: Path, csv_path: Path) -> int:
    relative_path = str(csv_path.relative_to(takeout_root))
    status = file_registry.check_file(conn, relative_path, csv_path)
    if not status.needs_ingest:
        return 0

    file_registry.begin_ingest(conn, relative_path, SOURCE_GROUP, csv_path, status.content_hash)
    file_registry.clear_prior_rows(conn, "gps_point", relative_path)

    # gps_location_YYYY-MM-DD.csv
    source_key = csv_path.stem.removeprefix("gps_location_")

    row_count = 0
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = []
            for seq, row in enumerate(reader):
                rows.append((
                    "gps_location_csv",
                    relative_path,
                    source_key,
                    row["timestamp"],
                    float(row["latitude"]),
                    float(row["longitude"]),
                    float(row["altitude"]) if row.get("altitude") not in (None, "") else None,
                    None,   # no cumulative distance in this source
                    None,   # no embedded HR in this source
                    seq,
                ))
            conn.executemany(
                """INSERT INTO gps_point
                   (source, source_file, source_key, point_time_utc, latitude, longitude,
                    altitude_m, distance_m, heart_rate, sequence_in_source)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
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


def ingest_all(conn: sqlite3.Connection, takeout_root: Path, physical_activity_dir: Path) -> int:
    total = 0
    for csv_path in sorted(physical_activity_dir.glob("gps_location_*.csv")):
        total += ingest_file(conn, takeout_root, csv_path)
    return total
