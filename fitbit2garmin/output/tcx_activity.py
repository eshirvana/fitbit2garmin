"""Generate a TCX file per activity. Secondary/optional output -- TCX's Sport
attribute is capped at Running/Biking/Other (the TCX schema has no Walking or
Swimming value either, confirmed against the TCX v2 XSD), which is exactly why
FIT (output/fit_activity.py) is authoritative for sport typing, not this.

Schema/element order matches a real Fitbit-exported TCX file, confirmed by direct
inspection of Activities/10303307188.tcx: Id, Lap(StartTime), TotalTimeSeconds,
DistanceMeters, Calories, Intensity, TriggerMethod, Track/Trackpoint(...).
"""

import sqlite3
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

from fit_tool.profile.profile_type import Sport

_TCX_NS = "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"

# TCX v2 XSD Sport_t enum is only {Running, Biking, Other} -- no Walking/Swimming.
_FIT_SPORT_TO_TCX_SPORT = {
    Sport.RUNNING: "Running",
    Sport.CYCLING: "Biking",
}


def _parse_utc(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _iso_z(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def write_activity_tcx(conn: sqlite3.Connection, activity_uid: str, output_dir: Path) -> Path:
    activity = conn.execute(
        "SELECT * FROM activity WHERE activity_uid = ?", (activity_uid,)
    ).fetchone()

    tcx_sport = _FIT_SPORT_TO_TCX_SPORT.get(Sport(activity["fit_sport"]), "Other")

    start_dt = _parse_utc(activity["start_time_utc"])

    root = ET.Element("TrainingCenterDatabase", {"xmlns": _TCX_NS})
    activities_el = ET.SubElement(root, "Activities")
    activity_el = ET.SubElement(activities_el, "Activity", {"Sport": tcx_sport})
    ET.SubElement(activity_el, "Id").text = _iso_z(start_dt)

    lap_el = ET.SubElement(activity_el, "Lap", {"StartTime": _iso_z(start_dt)})
    ET.SubElement(lap_el, "TotalTimeSeconds").text = str(activity["duration_s"])
    if activity["distance_m"] is not None:
        ET.SubElement(lap_el, "DistanceMeters").text = str(activity["distance_m"])
    if activity["calories"] is not None:
        ET.SubElement(lap_el, "Calories").text = str(int(activity["calories"]))
    ET.SubElement(lap_el, "Intensity").text = "Active"
    ET.SubElement(lap_el, "TriggerMethod").text = "Manual"

    points = conn.execute(
        """SELECT gp.point_time_utc, gp.latitude, gp.longitude, gp.altitude_m,
                  gp.distance_m, gp.heart_rate
           FROM activity_gps_point agp
           JOIN gps_point gp ON gp.id = agp.gps_point_id
           WHERE agp.activity_uid = ?
           ORDER BY agp.seq""",
        (activity_uid,),
    ).fetchall()

    if points:
        track_el = ET.SubElement(lap_el, "Track")
        for pt in points:
            tp_el = ET.SubElement(track_el, "Trackpoint")
            ET.SubElement(tp_el, "Time").text = _iso_z(_parse_utc(pt["point_time_utc"]))
            pos_el = ET.SubElement(tp_el, "Position")
            ET.SubElement(pos_el, "LatitudeDegrees").text = str(pt["latitude"])
            ET.SubElement(pos_el, "LongitudeDegrees").text = str(pt["longitude"])
            if pt["altitude_m"] is not None:
                ET.SubElement(tp_el, "AltitudeMeters").text = str(pt["altitude_m"])
            if pt["distance_m"] is not None:
                ET.SubElement(tp_el, "DistanceMeters").text = str(pt["distance_m"])
            if pt["heart_rate"] is not None:
                hr_el = ET.SubElement(tp_el, "HeartRateBpm")
                ET.SubElement(hr_el, "Value").text = str(int(pt["heart_rate"]))

    safe_name = activity["activity_name_raw"].replace(" ", "-").replace("/", "-")
    start_compact = activity["start_time_utc"].replace(":", "").replace("-", "").rstrip("Z")
    filename = f"{safe_name}_{activity_uid.replace(':', '_')}_{start_compact}.tcx"
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / filename

    tree = ET.ElementTree(root)
    ET.indent(tree, space="    ")
    tree.write(filepath, encoding="UTF-8", xml_declaration=True)
    return filepath
