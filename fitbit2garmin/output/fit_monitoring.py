"""FIT export for sleep and daily HR/SpO2/HRV -- CONFIRMED PROBLEMATIC against a
real Garmin Connect account, kept available but NOT called by default (see
cli.py's --include-fit-monitoring flag; output/csv_archive.py is the default
monitoring-data deliverable now). Two distinct, both-bad confirmed outcomes:

- write_sleep_fit (FileType.MONITORING_B): rejected outright by Garmin
  Connect's manual "Import Data" page -- "Sorry, your upload failed. Register
  your device, and try again." Garmin's manual-upload validation checks
  MONITORING_B files against registered devices in a way activity/weight
  uploads aren't subjected to. The only known workarounds spoof a real
  device's manufacturer/serial number; this project won't do that -- it's
  circumventing a device-authenticity control, not fixing a format bug.
- write_resting_hr_fit / write_spo2_fit / write_hrv_fit (FileType.ACTIVITY,
  one mini-session per daily reading): upload succeeds, but each reading shows
  up as its own fake zero-duration "activity" in the user's real Garmin
  activity history -- confirmed against a real account. Hundreds/thousands of
  daily readings means hundreds/thousands of junk activities polluting real
  data. Worse than not working.

Chunking at the UINT16 message-count ceiling (65535) is still applied
correctly (the old codebase's git history shows this was a reactively-fixed
crash bug -- see commit 1861a572) in case this code is ever revisited.
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from fit_tool.fit_file_builder import FitFileBuilder
from fit_tool.profile.messages.activity_message import ActivityMessage
from fit_tool.profile.messages.event_message import EventMessage
from fit_tool.profile.messages.file_id_message import FileIdMessage
from fit_tool.profile.messages.hrv_message import HrvMessage
from fit_tool.profile.messages.lap_message import LapMessage
from fit_tool.profile.messages.monitoring_info_message import MonitoringInfoMessage
from fit_tool.profile.messages.monitoring_message import MonitoringMessage
from fit_tool.profile.messages.record_message import RecordMessage
from fit_tool.profile.messages.session_message import SessionMessage
from fit_tool.profile.profile_type import (
    ActivityLevel,
    ActivityType as FitActivityType,
    Event,
    EventType,
    FileType,
    Manufacturer,
    Sport,
    SubSport,
)

_CHUNK_SIZE = 65535  # ActivityMessage.num_sessions / MonitoringInfoMessage counts are UINT16

_STAGE_TO_LEVEL = {
    "deep": ActivityLevel.LOW, "light": ActivityLevel.MEDIUM, "rem": ActivityLevel.MEDIUM,
    "wake": ActivityLevel.HIGH, "awake": ActivityLevel.HIGH,
    "asleep": ActivityLevel.LOW, "restless": ActivityLevel.MEDIUM,
}


def _parse_utc_ms(ts: str) -> int:
    return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000)


def write_sleep_fit(conn: sqlite3.Connection, output_path: Path) -> tuple[Path, int]:
    entries = conn.execute("SELECT * FROM sleep_entry ORDER BY start_time_utc").fetchall()

    builder = FitFileBuilder(auto_define=True, min_string_size=50)
    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

    file_id = FileIdMessage()
    file_id.type = FileType.MONITORING_B
    file_id.manufacturer = Manufacturer.DEVELOPMENT
    file_id.time_created = now_ms
    builder.add(file_id)

    info = MonitoringInfoMessage()
    info.timestamp = now_ms
    info.activity_type = [FitActivityType.SEDENTARY]
    builder.add(info)

    added = 0
    for entry in entries:
        stages = conn.execute(
            "SELECT * FROM sleep_stage WHERE sleep_entry_id=? ORDER BY start_time_utc", (entry["id"],)
        ).fetchall()

        if stages:
            for seg in stages:
                msg = MonitoringMessage()
                msg.timestamp = _parse_utc_ms(seg["start_time_utc"]) + seg["duration_s"] * 1000
                msg.activity_type = FitActivityType.SEDENTARY
                msg.active_time = float(seg["duration_s"])
                msg.activity_level = _STAGE_TO_LEVEL.get(seg["stage"].lower(), ActivityLevel.MEDIUM)
                builder.add(msg)
                added += 1
        elif entry["end_time_utc"] and entry["duration_ms"]:
            msg = MonitoringMessage()
            msg.timestamp = _parse_utc_ms(entry["end_time_utc"])
            msg.activity_type = FitActivityType.SEDENTARY
            msg.active_time = float(entry["duration_ms"]) / 1000
            msg.activity_level = ActivityLevel.LOW
            builder.add(msg)
            added += 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    builder.build().to_file(str(output_path))
    return output_path, added


def _write_daily_value_chunk(chunk: list[sqlite3.Row], set_field) -> FitFileBuilder:
    """One mini ACTIVITY session per daily value (EventStart/Record/EventStop/
    Lap/Session), matching the pattern the old codebase used for SpO2/HRV --
    heavier than a single RecordMessage per point, but this is the structure
    that's actually been round-trip-verified against fit-tool's decoder."""
    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    builder = FitFileBuilder(auto_define=True, min_string_size=50)

    file_id = FileIdMessage()
    file_id.type = FileType.ACTIVITY
    file_id.manufacturer = Manufacturer.DEVELOPMENT
    file_id.time_created = now_ms
    builder.add(file_id)

    for row in chunk:
        ts_dt = datetime.fromisoformat(row["ts_utc"].replace("Z", "+00:00"))
        day_start_ms = int(datetime(ts_dt.year, ts_dt.month, ts_dt.day, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        ts_ms = int(ts_dt.timestamp() * 1000)

        ev_start = EventMessage()
        ev_start.timestamp = day_start_ms
        ev_start.event = Event.TIMER
        ev_start.event_type = EventType.START
        builder.add(ev_start)

        rec = RecordMessage()
        rec.timestamp = ts_ms
        set_field(rec, row)
        builder.add(rec)

        ev_stop = EventMessage()
        ev_stop.timestamp = ts_ms + 1000
        ev_stop.event = Event.TIMER
        ev_stop.event_type = EventType.STOP_ALL
        builder.add(ev_stop)

        lap = LapMessage()
        lap.timestamp = ts_ms
        lap.start_time = day_start_ms
        lap.total_elapsed_time = (ts_ms - day_start_ms) / 1000.0
        builder.add(lap)

        session = SessionMessage()
        session.timestamp = ts_ms
        session.start_time = day_start_ms
        session.sport = Sport.GENERIC
        session.sub_sport = SubSport.GENERIC
        session.total_elapsed_time = (ts_ms - day_start_ms) / 1000.0
        session.num_laps = 1
        builder.add(session)

    act = ActivityMessage()
    act.timestamp = now_ms
    act.num_sessions = len(chunk)
    act.total_timer_time = 0.0
    builder.add(act)
    return builder


def write_daily_metric_fit(
    conn: sqlite3.Connection, metric_type: str, output_dir: Path, base_filename: str, set_field,
) -> tuple[list[Path], int]:
    rows = conn.execute(
        "SELECT * FROM monitoring_metric WHERE metric_type=? ORDER BY ts_utc", (metric_type,)
    ).fetchall()
    if not rows:
        return [], 0

    output_dir.mkdir(parents=True, exist_ok=True)
    chunks = [rows[i:i + _CHUNK_SIZE] for i in range(0, len(rows), _CHUNK_SIZE)]
    paths = []
    for i, chunk in enumerate(chunks):
        builder = _write_daily_value_chunk(chunk, set_field)
        suffix = "" if i == 0 else f"_{i + 1}"
        path = output_dir / f"{base_filename}{suffix}.fit"
        builder.build().to_file(str(path))
        paths.append(path)
    return paths, len(rows)


def write_resting_hr_fit(conn: sqlite3.Connection, output_dir: Path) -> tuple[list[Path], int]:
    return write_daily_metric_fit(
        conn, "resting_heart_rate", output_dir, "resting_hr",
        lambda rec, row: setattr(rec, "heart_rate", int(row["value"])),
    )


def write_spo2_fit(conn: sqlite3.Connection, output_dir: Path) -> tuple[list[Path], int]:
    return write_daily_metric_fit(
        conn, "spo2_daily", output_dir, "spo2",
        lambda rec, row: setattr(rec, "saturated_hemoglobin_percent", float(row["value"])),
    )


def write_hrv_fit(conn: sqlite3.Connection, output_dir: Path) -> tuple[list[Path], int]:
    """HRV has no daily-summary field in the FIT spec's HrvMessage -- it's built
    for raw beat-to-beat R-R interval times. Replicates the old codebase's
    approach (kept, not reinvented, since it's the confirmed-structurally-valid
    convention): store the daily RMSSD (ms) as a single-element `time` list, in
    seconds per the field's unit. This is a known representational compromise,
    not a full/correct use of the field -- flagged here and in the CLI output."""
    rows = conn.execute("SELECT * FROM monitoring_metric WHERE metric_type='hrv_daily' ORDER BY ts_utc").fetchall()
    if not rows:
        return [], 0

    output_dir.mkdir(parents=True, exist_ok=True)
    chunks = [rows[i:i + _CHUNK_SIZE] for i in range(0, len(rows), _CHUNK_SIZE)]
    paths = []
    for i, chunk in enumerate(chunks):
        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        builder = FitFileBuilder(auto_define=True, min_string_size=50)

        file_id = FileIdMessage()
        file_id.type = FileType.ACTIVITY
        file_id.manufacturer = Manufacturer.DEVELOPMENT
        file_id.time_created = now_ms
        builder.add(file_id)

        for row in chunk:
            ts_ms = _parse_utc_ms(row["ts_utc"])

            ev_start = EventMessage()
            ev_start.timestamp = ts_ms
            ev_start.event = Event.TIMER
            ev_start.event_type = EventType.START
            builder.add(ev_start)

            hrv_msg = HrvMessage()
            hrv_msg.time = [row["value"] / 1000.0]  # RMSSD ms -> the field's seconds unit
            builder.add(hrv_msg)

            ev_stop = EventMessage()
            ev_stop.timestamp = ts_ms + 1000
            ev_stop.event = Event.TIMER
            ev_stop.event_type = EventType.STOP_ALL
            builder.add(ev_stop)

            lap = LapMessage()
            lap.timestamp = ts_ms
            lap.start_time = ts_ms
            lap.total_elapsed_time = 1.0
            builder.add(lap)

            session = SessionMessage()
            session.timestamp = ts_ms
            session.start_time = ts_ms
            session.sport = Sport.GENERIC
            session.sub_sport = SubSport.GENERIC
            session.total_elapsed_time = 1.0
            session.num_laps = 1
            builder.add(session)

        act = ActivityMessage()
        act.timestamp = now_ms
        act.num_sessions = len(chunk)
        act.total_timer_time = 0.0
        builder.add(act)

        suffix = "" if i == 0 else f"_{i + 1}"
        path = output_dir / f"hrv{suffix}.fit"
        builder.build().to_file(str(path))
        paths.append(path)
    return paths, len(rows)
