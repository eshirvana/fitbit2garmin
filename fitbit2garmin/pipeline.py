"""Pipeline orchestration: ingest (raw files -> staging DB), reconcile (-> activity
table), output (-> Garmin-importable files). See PROGRESS.md for phase status.
"""

import logging
import sqlite3
from pathlib import Path

from .config import DEFAULT_DB_FILENAME, TakeoutLayout, discover_fitbit_root
from .db.connection import get_connection
from .db.migrations import migrate
from .ingest import exercise_json, gps_location_csv, tcx_activities, user_exercises
from .reconcile import activity_matcher

logger = logging.getLogger(__name__)


def open_db(db_path: Path) -> sqlite3.Connection:
    conn = get_connection(db_path)
    migrate(conn)
    return conn


def run_ingest(takeout_root: Path, db_path: Path) -> dict:
    """Ingest all currently-supported source formats (Phase 0/1 scope: activities +
    GPS). Idempotent -- unchanged files are skipped via the content-hash registry.
    """
    fitbit_root = discover_fitbit_root(takeout_root)
    layout = TakeoutLayout(fitbit_root=fitbit_root)
    conn = open_db(db_path)

    counts = {}
    logger.info("Ingesting UserExercises (driving activity set)...")
    counts["user_exercises"] = user_exercises.ingest_all(
        conn, fitbit_root, layout.health_fitness_data_google_data
    )
    logger.info("Ingesting classic exercise-*.json...")
    counts["exercise_json"] = exercise_json.ingest_all(
        conn, fitbit_root, layout.global_export_data
    )
    logger.info("Ingesting TCX GPS files...")
    counts["tcx"] = tcx_activities.ingest_all(conn, fitbit_root, layout.activities)
    logger.info("Ingesting gps_location day CSVs...")
    counts["gps_location_csv"] = gps_location_csv.ingest_all(
        conn, fitbit_root, layout.physical_activity_google_data
    )

    conn.close()
    return counts


def run_reconcile(db_path: Path) -> dict:
    """Rebuild the canonical `activity` table from ingested staging data. Idempotent
    -- safe to re-run after a matcher/mapping-table code change without re-ingesting.
    """
    conn = open_db(db_path)
    stats = activity_matcher.reconcile_all(conn)
    conn.close()
    return stats


def ingest_summary(db_path: Path) -> dict:
    """Row totals per staging table, for verification against known real-data counts."""
    conn = open_db(db_path)
    summary = {}
    for table in ("raw_user_exercise", "raw_exercise_json", "gps_point"):
        summary[table] = conn.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"]
    summary["gps_point_by_source"] = {
        row["source"]: row["n"]
        for row in conn.execute(
            "SELECT source, count(*) AS n FROM gps_point GROUP BY source"
        ).fetchall()
    }
    summary["tcx_files"] = conn.execute(
        "SELECT count(DISTINCT source_key) AS n FROM gps_point WHERE source='tcx'"
    ).fetchone()["n"]
    conn.close()
    return summary
