"""Generate a GPX file per activity. Secondary/optional output -- FIT is
authoritative for sport typing (see output/fit_activity.py); GPX carries no
sport-type concept at all, just the track."""

import sqlite3
from datetime import datetime
from pathlib import Path

import gpxpy
import gpxpy.gpx


def _parse_utc(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def write_activity_gpx(conn: sqlite3.Connection, activity_uid: str, output_dir: Path) -> Path | None:
    """Returns None (no file written) if the activity has no GPS -- GPX has
    nothing meaningful to carry without a track."""
    activity = conn.execute(
        "SELECT activity_name_raw, start_time_utc, gps_source FROM activity WHERE activity_uid = ?",
        (activity_uid,),
    ).fetchone()
    if activity["gps_source"] == "none":
        return None

    points = conn.execute(
        """SELECT gp.point_time_utc, gp.latitude, gp.longitude, gp.altitude_m
           FROM activity_gps_point agp
           JOIN gps_point gp ON gp.id = agp.gps_point_id
           WHERE agp.activity_uid = ?
           ORDER BY agp.seq""",
        (activity_uid,),
    ).fetchall()
    if not points:
        return None

    gpx = gpxpy.gpx.GPX()
    track = gpxpy.gpx.GPXTrack(name=activity["activity_name_raw"])
    gpx.tracks.append(track)
    segment = gpxpy.gpx.GPXTrackSegment()
    track.segments.append(segment)

    for pt in points:
        segment.points.append(
            gpxpy.gpx.GPXTrackPoint(
                latitude=pt["latitude"],
                longitude=pt["longitude"],
                elevation=pt["altitude_m"],
                time=_parse_utc(pt["point_time_utc"]),
            )
        )

    safe_name = activity["activity_name_raw"].replace(" ", "-").replace("/", "-")
    start_compact = activity["start_time_utc"].replace(":", "").replace("-", "").rstrip("Z")
    filename = f"{safe_name}_{activity_uid.replace(':', '_')}_{start_compact}.gpx"
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / filename
    filepath.write_text(gpx.to_xml())
    return filepath
