# fitbit2garmin

Migrate a Fitbit Google Takeout export to Garmin Connect: activities (with GPS
and correct sport type), weight/body composition, and best-effort daily/sleep/
HR/SpO2/HRV data.

Built as a from-scratch rewrite (v2.0) after real-world testing against a real
~3,900-activity, 2016–2025 Takeout export and a real Garmin Connect account —
every claim below (formats, units, quirks) is either directly confirmed against
that account or explicitly flagged as unconfirmed. See `PROGRESS.md` for the
full build history and every real bug found along the way.

## What this does, and doesn't do

- Reads your Takeout export, reconciles it into a clean per-activity/per-metric
  view in a local SQLite database, and generates files you manually import into
  Garmin Connect.
- **Never talks to Garmin's or Fitbit's servers.** No API keys, no OAuth, no
  upload automation. You control every import.
- Activities are the priority: correct sport type, GPS, and full detail. Weight
  is next. Sleep/HR/SpO2/HRV are exported as a personal-reference CSV archive
  only — the FIT import path was tried against a real Garmin Connect account
  and confirmed to make things worse, not better (see Status below).

## Status (as of this rewrite)

| Data type | Status |
|---|---|
| Activities (FIT/TCX/GPX) | ✅ Confirmed working against a real Garmin Connect account |
| Weight/BMI/body-fat (FIT) | ✅ Confirmed working against a real Garmin Connect account |
| Weight/BMI/body-fat (CSV) | ⚠️ Real bugs found and fixed (see below); not re-tested since FIT worked first |
| Daily totals CSV (steps/calories/distance/floors/active-minutes) | ⚠️ Format confirmed from a real reference implementation's source, not yet tested against Garmin |
| Sleep (FIT) | ❌ Confirmed **rejected** by Garmin Connect ("Sorry, your upload failed. Register your device, and try again.") — see below. CSV archive only by default. |
| Resting HR / SpO2 / HRV (FIT) | ❌ Confirmed to **upload successfully but pollute your real activity history** — each daily reading becomes its own fake zero-duration "activity". CSV archive only by default. |

## Installation

Requires Python 3.10+.

```bash
pip install -r requirements.txt
pip install -e .          # editable install, for development
```

Dependencies are intentionally minimal: `click` (CLI), `fit-tool` (FIT
read/write), `gpxpy` (GPX write). Everything else — CSV, JSON, XML parsing,
SQLite — is Python's standard library.

## Getting your data

1. Go to [Google Takeout](https://takeout.google.com/), select "Fitbit" (or
   "Google Health" if migrated), export as ZIP, and extract it.
2. Point every command below at the extracted folder (either the Takeout root
   or its `Fitbit` subfolder — both are auto-detected).

## Usage

### Activities (primary)

Always validate a small sample against your real Garmin Connect account before
running the full history:

```bash
# 1. Validation batch — a stratified sample (different sports, GPS/no-GPS mixes,
#    spread across years) so you're not just checking the easy cases
fitbit2garmin convert path/to/Takeout --sample 15 --output-dir ./output

# 2. Upload output/fit/*.fit to Garmin Connect and confirm sport type, GPS,
#    duration, calories, and HR look right

# 3. Full history, once confirmed
fitbit2garmin convert path/to/Takeout --output-dir ./output

# 4. If you have hundreds/thousands of activities, split into upload-sized
#    batches first — Garmin's web importer is unreliable with very large
#    single-shot batches (confirmed: a valid file failed generically as part
#    of a ~3,900-file batch, succeeded alone)
fitbit2garmin batch-output ./output/fit --batch-size 100
```

Upload one `batch_NNN/` folder at a time. `.fit` files are authoritative
(correct sport type for all mapped activity types); `.tcx`/`.gpx` are secondary
fallbacks — TCX's `Sport` field is capped at Running/Biking/Other by the TCX
schema itself, so most activity types show as "Other" there regardless.

### Weight / body composition

```bash
fitbit2garmin export-weight path/to/Takeout --output-dir ./output
```

Writes `output/weight.fit` — the confirmed-working path. Pass `--format csv` or
`--format both` to also (or instead) generate Garmin's official "Import Data
From Fitbit" CSV format, which needed several real bug fixes to even get past a
generic upload-failure error (see "Real bugs found" below) and hasn't been
re-tested against Garmin since FIT worked.

### Sleep / HR / SpO2 / HRV / daily totals (best-effort)

```bash
fitbit2garmin export-monitoring path/to/Takeout --output-dir ./output
```

Steps/calories/distance/floors are aggregated to **daily sums** (the source
files are minute-level; storing every reading would reproduce the exact
15GB+/19M-row memory problem this project was explicitly designed to avoid, for
data that's lowest priority to begin with).

Default output: `daily_totals_garmin_import.csv` (Garmin's official CSV format,
not yet confirmed working) plus `sleep_archive.csv`/`resting_hr_archive.csv`/
`spo2_archive.csv`/`hrv_archive.csv` — **personal-reference CSVs, not
Garmin-importable**. That's deliberate: the FIT path for these four was tried
against a real Garmin Connect account and confirmed to make things worse, not
better (sleep rejected outright, the other three upload but pollute your real
activity history — see Status above). It's still available via
`--include-fit-monitoring` if you want to experiment despite that, but the
default steers you away from it.

### Other commands

```bash
fitbit2garmin ingest path/to/Takeout          # just parse into SQLite, no output files
fitbit2garmin reconcile <db_path>              # re-run matching after a code change, no re-parsing
fitbit2garmin info                             # overview of commands and status
fitbit2garmin --help / <command> --help        # full flag reference
```

## Architecture

```
Takeout export
    → ingest/*.py        parse raw files into a SQLite staging DB (idempotent,
                          resumable via per-file content hash)
    → reconcile/*.py      join/dedupe into a canonical `activity` table
                          (see below — this is the hard part)
    → output/*.py         generate FIT/TCX/GPX/CSV from the canonical tables
```

Everything lands in one SQLite file (`fitbit2garmin.sqlite3` by default,
alongside your Takeout folder). It's a real database — `sqlite3
fitbit2garmin.sqlite3` lets you inspect exactly what happened to any activity,
which is also what the three Claude Code Skills in `.claude/skills/` do.

### Why activity reconciliation is non-trivial

A Fitbit Takeout export contains **three overlapping activity sources** that
don't agree with each other:
- `UserExercises_*.csv` (the driving set here) — includes silent auto-detected
  fragments Fitbit noticed but you never explicitly logged.
- `exercise-*.json` (classic Fitbit log) — narrower date range, has extra
  fields (heart rate zones, `hasGps` flag) but doesn't cover everything in
  `UserExercises`.
- GPS itself comes from **two more** independent sources: per-activity `.tcx`
  files (exact match by filename) and a continuous day-by-day location log
  (`gps_location_*.csv`) that has to be sliced to an activity's time window as
  a fallback.

These are joined by timestamp proximity (the two activity logs use unrelated ID
schemes), with every match candidate logged — not just the winner — so a wrong
activity type or missing GPS can always be traced back to *why*, not just
patched around. See `fitbit2garmin/reconcile/activity_matcher.py` and the
`fitbit2garmin-debug-activity-type` skill.

## Known limitations

- **Distance is genuinely absent for auto-detected activities.** Fitbit's
  SmartTrack auto-detection (silent activity recognition from wrist
  motion/heart-rate) never has GPS or a paired-phone location running, so there
  is no distance to migrate — confirmed 0/3,097 auto-detected activities in a
  real dataset have any distance recorded anywhere in the source data, vs.
  320/320 for actively-started (tracker) activities. Not a bug in this tool.
- **GPS coverage is sparse for many outdoor activities.** Fitbit's day-level
  location log only has data for days GPS happened to be running; many
  plausible-GPS activities (walks, bike rides) genuinely have no coordinates
  anywhere in the export.
- Peak heart rate is often a **zone-boundary estimate**, not a true measured
  peak, when only heart-rate-zone summaries (not per-second HR) are available
  for an activity.
- TCX's `Sport` field is capped at Running/Biking/Other by the schema itself —
  use the `.fit` files for correct sport type.
- Sleep/HR/SpO2/HRV import is best-effort by design — see Status above.

## Testing

```bash
pip install -r requirements-dev.txt
pytest tests/
```

Tests cover the reconciliation algorithm (time-matching tiers, claim conflicts,
sparse-fragment handling), the activity-type mapping table, FIT round-trip
encoding/decoding, CSV export format, and batch splitting — grounded in real
data shapes found during development, not just synthetic cases.

## Development

See `CLAUDE.md` for architecture details aimed at AI coding assistants working
in this repo, and `PROGRESS.md` for the phase-by-phase build history.

```
fitbit2garmin/
├── cli.py            Click CLI commands
├── config.py          Takeout directory discovery/layout
├── pipeline.py         Orchestration: ingest -> reconcile -> output
├── db/                 SQLite schema, connection, migrations
├── ingest/              One module per source file format -> staging tables
├── reconcile/            Activity matching, GPS attachment, type mapping
└── output/               FIT/TCX/GPX/CSV generation from canonical tables
```

## Disclaimer

Not affiliated with Fitbit or Garmin. Use at your own risk and keep a backup of
your original Takeout export.
