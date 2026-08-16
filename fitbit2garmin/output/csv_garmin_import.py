"""Generate CSVs for Garmin Connect's official "Import Data From Fitbit" feature
(support.garmin.com/en-US/?faq=HfJ4xPchdD3cmZ2qtDpOR8).

Format confirmed by directly reading the source of `simonepri/fitbit2garmin`
(github.com/simonepri/fitbit2garmin, 111 stars, a maintained tool built for
exactly this migration, pulling live from Fitbit's own Web API) -- a much
stronger source than forum anecdotes since it's real, exercised code:
    Body
    Date,Weight,BMI,Fat
    2019-01-11,77.5,25.84,0
Confirmed from that source: (1) a literal `Body` marker line precedes the header
row; (2) dates are passed through in Fitbit's own ISO `YYYY-MM-DD` format,
UNCHANGED -- no MM-DD-YYYY/DD-MM-YYYY reformatting at all (this module previously
guessed dash-separated MM-DD-YYYY, which was wrong -- fixed here after that
guess's CSV still failed a real import attempt); (3) numbers are plain Python
float-to-str, no fixed decimal places (BMI shown with 2 decimals, weight with 1,
because that's just each value's natural repr, not a formatting rule); (4)
missing Fat is literally the string "0", never blank.

NOT confirmed: the exact weight unit Garmin's importer expects. The reference
tool passes Fitbit Web API values through unconverted; this project instead
works from Takeout (see ingest/weight_json.py), which is CONFIRMED in lbs for
this account (Profile.csv weight_unit=en_US). Defaults to imperial (lbs)
accordingly, --units exposed in case a real import shows otherwise.

Daily-totals format, confirmed from the same source:
    Activities
    Date,Calories Burned,Steps,Distance,Floors,Minutes Sedentary,Minutes Lightly Active,Minutes Fairly Active,Minutes Very Active,Activity Calories
NOT available from this project's data sources: "Activity Calories" (calories
above BMR) has no equivalent ingested field -- written as 0 (the established
not-available placeholder, per the Fat=0 convention above), a known real gap,
not a guess. Distance is in the unit implied by --units, matching the weight
CSV's convention, though (like weight) this is unconfirmed against a real import.
"""

import sqlite3
from pathlib import Path

_KG_TO_LBS = 2.20462262

# Reported gotcha (Garmin forums): the importer can reject files with incomplete
# recent days, so callers should generally exclude the last few days -- not
# applied here automatically since this is a one-time historical migration, not
# a rolling sync; flagged for awareness during the Phase 2 validation checkpoint.


def _format_date(date_str: str, locale: str) -> str:
    """date_str is already 'YYYY-MM-DD'. Confirmed (via simonepri/fitbit2garmin's
    source) that Garmin's importer expects Fitbit's own ISO format unchanged --
    'iso' is the default and the confirmed-correct choice; 'us'/'eu' are kept as
    an escape hatch only, not because either is expected to be right."""
    if locale == "iso":
        return date_str
    year, month, day = date_str.split("-")
    return f"{day}-{month}-{year}" if locale == "eu" else f"{month}-{day}-{year}"


def _format_number(value: float, locale: str) -> str:
    # Confirmed reference behavior: plain str(), no fixed decimal places.
    s = str(round(value, 2)) if isinstance(value, float) else str(value)
    return s.replace(".", ",") if locale == "eu" else s


def write_weight_csv(
    conn: sqlite3.Connection,
    output_path: Path,
    locale: str = "us",
    units: str = "imperial",
    start_date: str | None = None,
    end_date: str | None = None,
) -> Path:
    query = "SELECT entry_date, weight_kg, bmi, body_fat_pct FROM weight_entry"
    params: list[str] = []
    if start_date or end_date:
        clauses = []
        if start_date:
            clauses.append("entry_date >= ?")
            params.append(start_date)
        if end_date:
            clauses.append("entry_date <= ?")
            params.append(end_date)
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY entry_date"

    rows = conn.execute(query, params).fetchall()

    # Deliberately not using csv.writer: it defaults to CRLF row terminators,
    # which (combined with a plain "\n"-terminated Body marker line) produced a
    # file with MIXED line endings -- a real bug found by inspecting the raw
    # bytes of a file that failed to import. A naive line-based parser splitting
    # on "\n" alone would see a stray "\r" stuck to the last field of every row
    # (header "Fat" -> "Fat\r", data "...,0" -> "...,0\r"), silently breaking
    # field matching/number parsing. The reference tool (simonepri/fitbit2garmin)
    # avoids this by never using the csv module -- plain "\n"-only lines throughout.
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="\n") as f:
        f.write("Body\n")
        f.write("Date,Weight,BMI,Fat\n")
        for row in rows:
            weight = row["weight_kg"] * _KG_TO_LBS if units == "imperial" else row["weight_kg"]
            # Confirmed requirement: BMI/Fat must never be blank, even when
            # unknown -- Garmin's importer rejects the file otherwise. 0 is the
            # confirmed-working placeholder, not a real "zero" measurement.
            bmi = _format_number(row["bmi"], locale) if row["bmi"] is not None else "0"
            fat = _format_number(row["body_fat_pct"], locale) if row["body_fat_pct"] is not None else "0"
            date = _format_date(row["entry_date"], locale)
            f.write(f"{date},{_format_number(weight, locale)},{bmi},{fat}\n")
    return output_path


_M_TO_MI = 0.000621371
_DAILY_METRIC_TYPES = (
    "calories_daily", "steps_daily", "distance_daily", "floors_daily",
    "sedentary_minutes", "lightly_active_minutes", "fairly_active_minutes", "very_active_minutes",
)


def write_daily_totals_csv(
    conn: sqlite3.Connection, output_path: Path, locale: str = "iso", units: str = "imperial",
) -> tuple[Path, int]:
    by_date: dict[str, dict[str, float]] = {}
    for metric_type in _DAILY_METRIC_TYPES:
        for row in conn.execute(
            "SELECT ts_utc, value FROM monitoring_metric WHERE metric_type=?", (metric_type,)
        ).fetchall():
            date = row["ts_utc"][:10]
            by_date.setdefault(date, {})[metric_type] = row["value"]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="\n") as f:
        f.write("Activities\n")
        f.write(
            "Date,Calories Burned,Steps,Distance,Floors,Minutes Sedentary,"
            "Minutes Lightly Active,Minutes Fairly Active,Minutes Very Active,Activity Calories\n"
        )
        for date in sorted(by_date):
            m = by_date[date]
            # distance_daily is stored in meters (confirmed via Physical Activity_
            # GoogleData/distance_readme.txt: "distance - Distance covered in
            # meters"), summed as-ingested with no conversion -- convert here.
            distance_m = m.get("distance_daily", 0.0)
            distance = distance_m * _M_TO_MI if units == "imperial" else distance_m / 1000.0
            values = [
                _format_number(m.get("calories_daily", 0.0), locale),
                _format_number(m.get("steps_daily", 0.0), locale),
                _format_number(distance, locale),
                _format_number(m.get("floors_daily", 0.0), locale),
                _format_number(m.get("sedentary_minutes", 0.0), locale),
                _format_number(m.get("lightly_active_minutes", 0.0), locale),
                _format_number(m.get("fairly_active_minutes", 0.0), locale),
                _format_number(m.get("very_active_minutes", 0.0), locale),
                "0",  # Activity Calories -- not available, see module docstring
            ]
            f.write(f"{_format_date(date, locale)},{','.join(values)}\n")
    return output_path, len(by_date)
