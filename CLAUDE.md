# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Python CLI tool (v2.0.0) that migrates a Fitbit Google Takeout export to Garmin
Connect: activities (GPS + correct sport type), weight/body composition, and
best-effort sleep/HR/SpO2/HRV/daily-totals data. Entry point:
`fitbit2garmin.cli:main`. Never talks to Garmin's or Fitbit's servers — it only
generates files for manual import.

This is a from-scratch rewrite (see `PROGRESS.md` for the full build history).
Real bugs were found and fixed by testing against an actual ~3,900-activity
Takeout export and a real Garmin Connect account — README.md documents every
one. When making changes here, prefer verifying against real data shapes over
assuming a format from documentation; this codebase has repeatedly found that
Fitbit/Garmin's real behavior differs from what's documented or expected
(units, date formats, required marker lines, batch-upload limits).

## Development Commands

```bash
# Install dependencies
pip install -r requirements.txt
pip install -e .                          # editable install

# Run the tool
fitbit2garmin convert path/to/Takeout --sample 15   # validation batch first
fitbit2garmin convert path/to/Takeout                # full run
fitbit2garmin export-weight path/to/Takeout
fitbit2garmin export-monitoring path/to/Takeout      # best-effort
fitbit2garmin batch-output ./output/fit --batch-size 100
fitbit2garmin ingest path/to/Takeout                 # parse only, no output files
fitbit2garmin reconcile <db_path>                    # re-match without re-parsing
```

Tests: `pip install -r requirements-dev.txt` then
```bash
uv run pytest tests/                                       # all tests
uv run pytest tests/test_reconcile_activity_matcher.py -v
```

## Architecture

```
Takeout export
    → ingest/*.py        raw files -> SQLite staging tables (idempotent, resumable
                          via per-file content hash in ingest_file)
    → reconcile/*.py      staging tables -> canonical `activity` table
    → output/*.py          canonical tables -> FIT/TCX/GPX/CSV files
```

Orchestrated by `pipeline.py`, invoked from `cli.py`. Everything lands in one
SQLite file (`db/schema.sql` defines the full schema) — inspect it directly
with `sqlite3 <db_path>` rather than re-deriving state from raw files.

### Module responsibilities

- **`config.py`**: `discover_fitbit_root()` finds the `Fitbit` folder under a
  Takeout export (handles either the Takeout root or the Fitbit folder itself
  being passed in). `TakeoutLayout` resolves the known subdirectory names.
- **`db/schema.sql`**: Full schema — `ingest_file` (resume registry),
  `raw_user_exercise`/`raw_exercise_json` (the two activity sources, joined
  later), `gps_point`/`activity_gps_point`, `activity` (canonical, reconciled),
  `reconciliation_log` (full audit trail of every match candidate, not just the
  winner), `weight_entry`, `monitoring_metric`, `sleep_entry`/`sleep_stage`.
- **`db/migrations.py`**: Applies `schema.sql` plus any `db/migrations/NNN_*.sql`
  files in order, tracked via a `schema_version` table.
- **`ingest/`**: One module per source file format. Each owns exactly one
  format and is independently re-runnable — `user_exercises.py`,
  `exercise_json.py`, `tcx_activities.py`, `gps_location_csv.py`,
  `weight_json.py`, `monitoring_csv.py`, `monitoring_json.py`. All go through
  `file_registry.py` for the sha256-based resume check (skip unchanged files,
  re-ingest changed ones idempotently).
- **`reconcile/activity_matcher.py`**: The core logic. `UserExercises_*.csv` is
  the driving set (every row becomes an activity, including sparse
  auto-detected fragments with blank tracker fields — a hard invariant is that
  every activity row has non-null start/end/duration/sport, even when nothing
  else is known). Matched to `exercise-*.json` by timestamp proximity (±5s
  exact tier, ±120s fuzzy tier) — the two sources use unrelated ID schemes.
  Field-level fallback chains (not row-level) pick the best available value per
  metric. Every match candidate is logged to `reconciliation_log`, not just the
  winner, including double-claim conflicts (first-come-first-served by start
  time, logged rather than silently resolved).
- **`reconcile/gps_attacher.py`**: Exact-filename TCX match first (trust file
  presence over the source's own `hasGps` flag, which can be stale/wrong), then
  a ±30s time-window fallback against the continuous `gps_location_csv` day-log
  for GPS-plausible activity types.
- **`reconcile/activity_type_map.py`**: Two static dicts (`activity_name` ->
  Sport/SubSport, `activity_type_id` -> Sport/SubSport, the latter taking
  precedence when both are available) plus a GPS-refinement rule applied after
  lookup (GENERIC cycling/running upgrades to ROAD/STREET when GPS is
  attached). Both tables were derived from real data actually present in a test
  export, not a generic ID list — if a new activity type shows up unmapped, add
  it here directly rather than building a fallback elsewhere.
- **`output/fit_activity.py`**: Primary activity output. Carries forward
  hard-learned FIT encoding requirements as day-one invariants, not reactive
  fixes: raw lat/lon in degrees (fit-tool converts to semicircles internally —
  never pre-convert), altitude/speed clamped to their UINT16-encodable ranges
  before assignment, one bad GPS point never aborts the whole file (per-point
  try/except with a skipped-count in the return report), record-count chunking
  guard at the FIT UINT16 ceiling (65,535). Steps are written as
  `total_strides = steps // 2` — FIT has no `total_steps` field on
  Session/Lap messages; setting a non-existent attribute on a `fit-tool`
  message class silently no-ops rather than erroring, so this kind of mismatch
  won't surface as a crash — verify against the actual installed message class
  attributes (`dir(SomeMessage())`) when adding new fields, don't assume a
  field exists from the FIT spec alone.
- **`output/tcx_activity.py`** / **`output/gpx_activity.py`**: Secondary
  formats. TCX's `Sport` attribute is capped at Running/Biking/Other by the TCX
  v2 schema itself (confirmed against the real XSD) — this is exactly why FIT
  is authoritative, not a bug to chase in the TCX writer.
- **`output/fit_weight.py`**: Confirmed-working against a real Garmin Connect
  account. Timestamps use **noon UTC of the entry's date**, not the actual
  logged time — deliberate, so Garmin buckets the measurement into the correct
  calendar day regardless of account timezone.
- **`output/fit_monitoring.py`**: Best-effort sleep/resting-HR/SpO2/HRV. HRV
  uses `HrvMessage.time` (a raw R-R-interval field, repurposed to carry a daily
  RMSSD value as a single-element list) since FIT has no dedicated daily-HRV
  summary field — a known representational compromise, not a full/correct use
  of the field.
- **`output/csv_garmin_import.py`**: Garmin's official "Import Data From
  Fitbit" CSV format. Format was reverse-engineered from a real, maintained
  open-source tool's source (`simonepri/fitbit2garmin` on GitHub) after forum
  guesses repeatedly failed — see the module docstring and README.md's "Real
  bugs found" section before changing anything here. Two non-obvious hard
  requirements: a literal marker line (`Body` or `Activities`) before the
  header row, and every field must be populated (write `0`, never leave blank)
  or Garmin's importer rejects the whole file. Written with plain `\n`-only
  lines, deliberately not using `csv.writer` (its default CRLF row terminator
  mixed with an LF marker line broke a real import).
- **`pipeline.py`**: Orchestration + `split_into_batches()` (Garmin's web
  importer is unreliable with very large single-batch uploads — confirmed
  against a real account, not a defensive guess).
- **`cli.py`**: Click commands — thin wrappers around `pipeline.py`.

### Adding a new data type

1. Add an `ingest/*.py` module (or a function in an existing one) that parses
   the source file(s) into a new/existing table in `db/schema.sql`, going
   through `ingest/file_registry.py` for resume tracking.
2. If it needs reconciliation against another source (like activities do), add
   the matching logic to `reconcile/`; if it's a direct passthrough (like
   weight), it can go straight from ingest to output.
3. Add an `output/*.py` function to generate the Garmin-importable file. Verify
   the actual target format against real evidence (a working example, a
   maintained reference implementation's source, or direct confirmation from a
   real import attempt) — this project has repeatedly found forum guesses and
   even official-sounding documentation to be wrong on specifics (units, date
   formats, required fields).
4. Wire it into `pipeline.py` and add a CLI command/flag in `cli.py`.
5. Add tests grounded in real data shapes, not just synthetic happy-path cases
   — see `tests/test_reconcile_activity_matcher.py`/`test_fit_roundtrip.py` for
   the pattern (real bug classes each test locks in, documented in the
   docstring/comments).
6. Update `PROGRESS.md` and `README.md`'s Status table.

### Claude Code Skills

`.claude/skills/` has three skills for working with this tool:
`fitbit2garmin-convert` (running conversions), `fitbit2garmin-validate-output`
(decoding generated files and cross-checking against the SQLite source data),
`fitbit2garmin-debug-activity-type` (tracing the reconciliation audit trail for
a specific activity). Use these instead of re-deriving the same debugging steps
ad hoc.
