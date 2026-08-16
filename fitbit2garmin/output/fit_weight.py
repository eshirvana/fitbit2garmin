"""Generate weight.fit (WeightScaleMessage per entry).

CONFIRMED WORKING by the user against a real Garmin Connect account -- this
replicates that confirmed file's structure exactly (found via a prior run of the
old codebase's `DataConverter.convert_body_composition_to_fit`, inspected via
fit-tool's decoder): FileId type=WEIGHT/manufacturer=DEVELOPMENT, one
WeightScaleMessage per entry with weight in kg, timestamped at **noon UTC of the
entry's date** rather than the entry's actual logged time -- deliberate, so
Garmin Connect buckets the measurement into the correct calendar day regardless
of the account's timezone, instead of a late-night entry (e.g. 23:59:59) rolling
into the wrong day. This is the primary/preferred weight import path (Phase 2) --
CSV (output/csv_garmin_import.py) is kept as a secondary/experimental option
since it needed multiple real-bug fixes and is less certain overall.
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from fit_tool.fit_file_builder import FitFileBuilder
from fit_tool.profile.messages.file_id_message import FileIdMessage
from fit_tool.profile.messages.weight_scale_message import WeightScaleMessage
from fit_tool.profile.profile_type import FileType, Manufacturer


def write_weight_fit(conn: sqlite3.Connection, output_path: Path) -> tuple[Path, int]:
    rows = conn.execute(
        "SELECT entry_date, weight_kg, bmi, body_fat_pct FROM weight_entry ORDER BY entry_date"
    ).fetchall()

    builder = FitFileBuilder(auto_define=True, min_string_size=50)

    file_id = FileIdMessage()
    file_id.type = FileType.WEIGHT
    file_id.manufacturer = Manufacturer.DEVELOPMENT
    file_id.time_created = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    builder.add(file_id)

    added = 0
    for row in rows:
        year, month, day = (int(p) for p in row["entry_date"].split("-"))
        noon_utc = datetime(year, month, day, 12, 0, 0, tzinfo=timezone.utc)

        ws = WeightScaleMessage()
        ws.timestamp = int(noon_utc.timestamp() * 1000)
        ws.weight = round(row["weight_kg"], 2)
        if row["body_fat_pct"] is not None:
            ws.percent_fat = float(row["body_fat_pct"])
        builder.add(ws)
        added += 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fit_file = builder.build()
    fit_file.to_file(str(output_path))
    return output_path, added
