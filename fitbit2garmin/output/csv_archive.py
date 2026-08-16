"""Personal-reference CSV archive for sleep/HR/SpO2/HRV data.

NOT importable into Garmin Connect -- this exists because the FIT-based
monitoring export path was tried and confirmed broken, not as a placeholder:
- sleep.fit (FileType.MONITORING_B) is rejected outright by Garmin Connect's
  manual "Import Data" page with "Sorry, your upload failed. Register your
  device, and try again." -- confirmed against a real account. Garmin's manual
  upload validates MONITORING_B files against registered devices in a way
  activity/weight uploads aren't; the only known workarounds spoof a real
  device's manufacturer/serial number, which this project won't do.
- resting_hr.fit / spo2.fit / hrv.fit (FileType.ACTIVITY, one mini-session per
  daily reading) DO upload successfully, but each reading shows up as its own
  fake zero-duration "activity" in the user's real Garmin activity history --
  confirmed against a real account. Hundreds/thousands of daily readings would
  mean hundreds/thousands of junk activities. Worse than not working at all.

See fitbit2garmin/output/fit_monitoring.py for the FIT implementations, kept
available but not called by default -- see cli.py's --include-fit-monitoring
flag if you want to experiment despite the above.
"""

import csv
import sqlite3
from pathlib import Path


def write_sleep_csv(conn: sqlite3.Connection, output_path: Path) -> tuple[Path, int]:
    rows = conn.execute(
        """SELECT log_id, start_time_utc, end_time_utc, duration_ms, efficiency,
                  minutes_asleep, minutes_awake, type
           FROM sleep_entry ORDER BY start_time_utc"""
    ).fetchall()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "log_id", "start_time_utc", "end_time_utc", "duration_minutes",
            "efficiency", "minutes_asleep", "minutes_awake", "type",
        ])
        for row in rows:
            duration_min = (row["duration_ms"] / 60000.0) if row["duration_ms"] else ""
            writer.writerow([
                row["log_id"], row["start_time_utc"], row["end_time_utc"],
                f"{duration_min:.1f}" if duration_min != "" else "",
                row["efficiency"] or "", row["minutes_asleep"] or "",
                row["minutes_awake"] or "", row["type"] or "",
            ])
    return output_path, len(rows)


def write_daily_metric_csv(
    conn: sqlite3.Connection, metric_type: str, output_path: Path, value_label: str,
) -> tuple[Path, int]:
    rows = conn.execute(
        "SELECT ts_utc, value FROM monitoring_metric WHERE metric_type=? ORDER BY ts_utc",
        (metric_type,),
    ).fetchall()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", value_label])
        for row in rows:
            writer.writerow([row["ts_utc"][:10], row["value"]])
    return output_path, len(rows)


def write_all(conn: sqlite3.Connection, output_dir: Path) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    results = {}
    path, n = write_sleep_csv(conn, output_dir / "sleep_archive.csv")
    results["sleep"] = (path, n)
    for metric_type, filename, label in (
        ("resting_heart_rate", "resting_hr_archive.csv", "resting_hr_bpm"),
        ("spo2_daily", "spo2_archive.csv", "avg_spo2_percent"),
        ("hrv_daily", "hrv_archive.csv", "avg_hrv_rmssd_ms"),
    ):
        path, n = write_daily_metric_csv(conn, metric_type, output_dir / filename, label)
        results[metric_type] = (path, n)
    return results
