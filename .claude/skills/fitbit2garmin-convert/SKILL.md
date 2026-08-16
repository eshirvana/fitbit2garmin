---
name: fitbit2garmin-convert
description: Run the fitbit2garmin CLI to convert Fitbit Takeout data (activities, weight, or monitoring data) into Garmin-importable files. Use when the user asks to convert, migrate, run, or export their Fitbit data to Garmin, run a validation batch, or do the full historical migration.
---

# fitbit2garmin: run a conversion

This project migrates a Fitbit Google Takeout export to Garmin Connect via three
independent CLI commands, each covering one data domain. Run them with `uv run
fitbit2garmin <command> ...` from the repo root. See `PROGRESS.md` for full
architecture/status context before making judgment calls.

## Step 1: figure out what's actually being asked for

Ask yourself (don't guess):
- **Which data domain?** Activities (`convert`), Weight (`export-weight`), or
  monitoring/daily-totals (`export-monitoring`)? A user request like "convert my
  data" without qualification most likely means Activities — that's this
  project's primary deliverable — but confirm if ambiguous.
- **Validation batch or full run?** If the user hasn't already validated this
  data type against their real Garmin Connect account (check `PROGRESS.md`'s
  phase status — Phase 1/Activities and Phase 2/Weight are both confirmed
  working as of the last update; Phase 3/monitoring is not), default to a small
  sample first (`--sample N` for `convert`, `--sample-days N` for
  `export-weight`'s CSV path). Never silently run a full, unbounded export for a
  data type that hasn't been validated yet without telling the user that's what
  you're doing and why.

## Step 2: run it

```bash
# Activities (primary) -- validation batch first
uv run fitbit2garmin convert <takeout_dir> --db ./fitbit2garmin.sqlite3 --output-dir ./output --sample 15

# Activities -- full run (only after the user has confirmed a sample batch works)
uv run fitbit2garmin convert <takeout_dir> --db ./fitbit2garmin.sqlite3 --output-dir ./output

# Weight -- FIT is the confirmed-working primary path, CSV is secondary/experimental
uv run fitbit2garmin export-weight <takeout_dir> --db ./fitbit2garmin.sqlite3 --output-dir ./output
uv run fitbit2garmin export-weight <takeout_dir> --format csv --sample-days 30 ...  # only if asked for CSV specifically

# Monitoring (best-effort, lowest priority)
uv run fitbit2garmin export-monitoring <takeout_dir> --db ./fitbit2garmin.sqlite3 --output-dir ./output
```

`--db` and `--output-dir` default sensibly (db inside the takeout dir, output to
`./output`) but prefer being explicit so re-runs are predictable and the ingest
resume cache (keyed by file content hash, not just presence) is reused correctly.

## Step 3: read the output, don't just report "done"

Every command prints a QA summary. Surface it to the user, don't just say
"conversion complete":
- `convert` prints match/GPS/unmapped-type counts. If `unmapped activity types`
  is nonzero, the run will refuse to proceed past a full (non-`--sample`) run
  unless `--allow-unmapped` is passed. **Never pass `--allow-unmapped` on the
  user's behalf without first telling them what's unmapped** — query
  `SELECT DISTINCT activity_name_raw, activity_type_id FROM activity WHERE
  reconciliation_notes LIKE '%unmapped%'` and show them, or point them at the
  `fitbit2garmin-debug-activity-type` skill.
- `export-monitoring` always prints an explicit best-effort disclaimer — repeat
  it to the user rather than letting "wrote N records" read as "will show up in
  Garmin."

## Step 4: point to the actual next action

The tool never uploads anything itself (no Garmin API, by design — see
PROGRESS.md decision #3). Always end by telling the user which files to
drag into Garmin Connect's Import Data page (`.fit` files primarily; `.tcx`/
`.gpx` are secondary fallbacks for activities) or the "Import Data From Fitbit"
wizard (weight/daily-totals CSV).
