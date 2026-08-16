"""Pipeline orchestration: ingest (raw files -> staging DB), reconcile (-> activity
table), output (-> Garmin-importable files). See PROGRESS.md for phase status.
"""

import logging
import sqlite3
from pathlib import Path

from .config import DEFAULT_DB_FILENAME, TakeoutLayout, discover_fitbit_root
from .db.connection import get_connection
from .db.migrations import migrate
from .ingest import (
    exercise_json,
    gps_location_csv,
    monitoring_csv,
    monitoring_json,
    tcx_activities,
    user_exercises,
    weight_json,
)
from .output import csv_archive, csv_garmin_import, fit_activity, fit_monitoring, fit_weight, gpx_activity, tcx_activity
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


def run_ingest_weight(takeout_root: Path, db_path: Path) -> int:
    fitbit_root = discover_fitbit_root(takeout_root)
    layout = TakeoutLayout(fitbit_root=fitbit_root)
    conn = open_db(db_path)
    count = weight_json.ingest_all(conn, fitbit_root, layout.global_export_data)
    conn.close()
    return count


def run_ingest_monitoring(takeout_root: Path, db_path: Path) -> dict:
    fitbit_root = discover_fitbit_root(takeout_root)
    layout = TakeoutLayout(fitbit_root=fitbit_root)
    conn = open_db(db_path)
    counts = {}
    counts.update(monitoring_csv.ingest_all(conn, fitbit_root, layout.physical_activity_google_data))
    counts.update(monitoring_json.ingest_all(conn, fitbit_root, layout.global_export_data))
    conn.close()
    return counts


def export_monitoring_fit(db_path: Path, output_dir: Path) -> dict:
    conn = open_db(db_path)
    results = {}
    path, n = fit_monitoring.write_sleep_fit(conn, output_dir / "sleep.fit")
    results["sleep"] = {"paths": [path], "count": n}
    for name, fn in (
        ("resting_hr", fit_monitoring.write_resting_hr_fit),
        ("spo2", fit_monitoring.write_spo2_fit),
        ("hrv", fit_monitoring.write_hrv_fit),
    ):
        paths, n = fn(conn, output_dir)
        results[name] = {"paths": paths, "count": n}
    conn.close()
    return results


def export_monitoring_archive(db_path: Path, output_dir: Path) -> dict:
    """Personal-reference CSV export (not Garmin-importable) -- the default
    monitoring-data deliverable, replacing the FIT path after confirming it
    either fails outright (sleep) or pollutes the user's real activity history
    (resting HR/SpO2/HRV) -- see output/csv_archive.py for the full story."""
    conn = open_db(db_path)
    results = csv_archive.write_all(conn, output_dir)
    conn.close()
    return results


def export_daily_totals_csv(
    db_path: Path, output_path: Path, locale: str = "iso", units: str = "imperial",
) -> tuple[Path, int]:
    conn = open_db(db_path)
    path, n = csv_garmin_import.write_daily_totals_csv(conn, output_path, locale=locale, units=units)
    conn.close()
    return path, n


def export_weight_fit(db_path: Path, output_path: Path) -> tuple[Path, int]:
    conn = open_db(db_path)
    path, n = fit_weight.write_weight_fit(conn, output_path)
    conn.close()
    return path, n


def export_weight_csv(
    db_path: Path, output_path: Path, locale: str = "us", units: str = "imperial",
    sample_days: int | None = None,
) -> tuple[Path, int]:
    conn = open_db(db_path)
    start_date = end_date = None
    if sample_days:
        row = conn.execute("SELECT entry_date FROM weight_entry ORDER BY entry_date LIMIT 1").fetchone()
        if row:
            from datetime import datetime, timedelta
            start = datetime.strptime(row["entry_date"], "%Y-%m-%d")
            start_date = start.strftime("%Y-%m-%d")
            end_date = (start + timedelta(days=sample_days)).strftime("%Y-%m-%d")
    path = csv_garmin_import.write_weight_csv(
        conn, output_path, locale=locale, units=units, start_date=start_date, end_date=end_date
    )
    n = conn.execute(
        "SELECT count(*) AS n FROM weight_entry" + (
            " WHERE entry_date BETWEEN ? AND ?" if sample_days else ""
        ),
        (start_date, end_date) if sample_days else (),
    ).fetchone()["n"]
    conn.close()
    return path, n


def select_validation_sample(conn: sqlite3.Connection, n: int) -> list[str]:
    """Stratified sample: one per fit_sport bucket, forced mix of gps_source
    values, at least one sparse (has_metrics=0) row, spread across years -- so a
    small manual Garmin Connect check exercises every real code path, not just the
    common case. See project plan / PROGRESS.md Phase 1 validation-batch workflow.
    """
    picked: dict[str, None] = {}  # dict as an ordered set

    def _add(rows):
        for r in rows:
            picked.setdefault(r["activity_uid"], None)

    _add(conn.execute(
        "SELECT activity_uid, min(start_time_utc) FROM activity GROUP BY fit_sport"
    ).fetchall())
    for gps_source in ("tcx", "gps_location_csv", "none"):
        _add(conn.execute(
            "SELECT activity_uid FROM activity WHERE gps_source=? LIMIT 1", (gps_source,)
        ).fetchall())
    _add(conn.execute(
        "SELECT activity_uid FROM activity WHERE has_metrics=0 LIMIT 1"
    ).fetchall())
    _add(conn.execute(
        """SELECT activity_uid FROM activity
           GROUP BY substr(start_time_utc, 1, 4)
           ORDER BY substr(start_time_utc, 1, 4)"""
    ).fetchall())

    uids = list(picked.keys())
    if len(uids) > n:
        # Keep the fit_sport/gps_source/sparse-row diversity picks (front of the
        # list) over the year-spread padding (appended last).
        uids = uids[:n]
    return uids


def generate_activity_outputs(
    conn: sqlite3.Connection, activity_uids: list[str], output_dir: Path, formats: set[str]
) -> dict:
    output_dir = Path(output_dir)
    results = {"written": 0, "points_written": 0, "points_skipped": 0, "errors": []}
    for uid in activity_uids:
        try:
            if "fit" in formats:
                _, report = fit_activity.write_activity_fit(conn, uid, output_dir / "fit")
                results["points_written"] += report["points_written"]
                results["points_skipped"] += report["points_skipped"]
            if "tcx" in formats:
                tcx_activity.write_activity_tcx(conn, uid, output_dir / "tcx")
            if "gpx" in formats:
                gpx_activity.write_activity_gpx(conn, uid, output_dir / "gpx")
            results["written"] += 1
        except Exception as exc:
            logger.error("Failed to generate output for %s: %s", uid, exc)
            results["errors"].append({"activity_uid": uid, "error": str(exc)})
    return results


def split_into_batches(directory: Path, batch_size: int) -> list[Path]:
    """Move files directly inside `directory` into batch_NNN/ subfolders of at
    most `batch_size` each -- Garmin Connect's web importer is unreliable with
    large single-batch uploads (confirmed: an otherwise-valid file failed with a
    generic error only when uploaded as part of a ~3900-file batch, succeeded
    alone), so splitting into upload-sized batches is a real workaround, not
    cosmetic. Files already inside a batch_NNN/ subfolder are left alone
    (idempotent re-run after partially re-batching)."""
    directory = Path(directory)
    files = sorted(p for p in directory.iterdir() if p.is_file())
    if not files:
        return []

    batch_dirs = []
    for i in range(0, len(files), batch_size):
        batch_num = i // batch_size + 1
        batch_dir = directory / f"batch_{batch_num:03d}"
        batch_dir.mkdir(exist_ok=True)
        for f in files[i:i + batch_size]:
            f.rename(batch_dir / f.name)
        batch_dirs.append(batch_dir)
    return batch_dirs


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
