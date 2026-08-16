"""
Command-line interface for the Fitbit to Garmin migration tool.
"""

import click
import logging
from pathlib import Path

from . import __version__
from . import pipeline
from .config import DEFAULT_DB_FILENAME

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@click.group()
@click.version_option(version=__version__)
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
def cli(verbose):
    """Fitbit to Garmin data migration tool.

    Convert Fitbit Google Takeout data to Garmin Connect compatible formats.
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")


@cli.command()
@click.argument("takeout_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--output-dir", "-o", type=click.Path(path_type=Path), default="./output",
    help="Output directory for converted files",
)
@click.option(
    "--db", "db_path", type=click.Path(path_type=Path), default=None,
    help=f"SQLite staging DB path (default: <takeout_path>/{DEFAULT_DB_FILENAME})",
)
@click.option(
    "--formats", "formats", default="fit,tcx,gpx",
    help="Comma-separated activity output formats to generate (fit,tcx,gpx). FIT is authoritative for sport type.",
)
@click.option(
    "--sample", "sample_n", type=int, default=None,
    help="Validation-batch mode: only generate output for a stratified sample of N activities "
         "(one per sport, mixed GPS sources, a sparse row, spread across years). "
         "Manually upload these to Garmin Connect and confirm correctness before running without --sample.",
)
@click.option(
    "--allow-unmapped", is_flag=True,
    help="Proceed with a full (non-sample) run even if some activity types have no Sport/SubSport mapping "
         "(they fall back to Generic/Generic). Without this flag, a full run is blocked until reviewed.",
)
def convert(takeout_path, output_dir, db_path, formats, sample_n, allow_unmapped):
    """Convert a Fitbit Takeout export's Activities to Garmin FIT/TCX/GPX.

    TAKEOUT_PATH: path to the extracted Google Takeout folder (or its Fitbit subfolder).

    Runs the full ingest -> reconcile -> output pipeline. This is the primary
    command -- Activities are this tool's main deliverable. See `export-weight`
    and `export-monitoring` for the other data domains.
    """
    db_path = Path(db_path) if db_path else Path(takeout_path) / DEFAULT_DB_FILENAME
    output_path = Path(output_dir)
    format_set = {f.strip() for f in formats.split(",") if f.strip()}
    unknown = format_set - {"fit", "tcx", "gpx"}
    if unknown:
        click.echo(f"❌ Unknown format(s): {', '.join(unknown)} (valid: fit, tcx, gpx)", err=True)
        raise SystemExit(1)

    click.echo(f"🔄 fitbit2garmin v{__version__} (activities pipeline)")
    click.echo(f"📁 Input: {takeout_path}")
    click.echo(f"🗄️  Staging DB: {db_path}")

    try:
        click.echo("\n📖 Ingesting...")
        counts = pipeline.run_ingest(Path(takeout_path), db_path)
        for source, n in counts.items():
            if n:
                click.echo(f"   {source}: {n} new rows")

        click.echo("\n🔗 Reconciling activities...")
        stats = pipeline.run_reconcile(db_path)
        click.echo(f"   Total activities: {stats['total']} (skipped: {stats['skipped']})")
        click.echo(
            f"   match: exact={stats['match_time_exact']} fuzzy={stats['match_time_fuzzy']} "
            f"unmatched={stats['match_user_exercises_only']}"
        )
        click.echo(
            f"   gps: tcx-exact={stats['gps_exact']} windowed={stats['gps_windowed']} "
            f"no_data={stats['gps_flagged_no_data']} not_expected={stats['gps_not_expected']}"
        )
        if stats["unmapped_type"]:
            click.echo(f"   ⚠️  {stats['unmapped_type']} activities have an unmapped type (-> Generic/Generic)")
        if stats["orphan_exercise_json_count"]:
            click.echo(
                f"   ℹ️  {stats['orphan_exercise_json_count']} exercise_json records had no matching "
                "UserExercises row (not included as activities)"
            )

        if stats["unmapped_type"] and not sample_n and not allow_unmapped:
            click.echo(
                "\n❌ Full run blocked: unmapped activity types present. Review them "
                "(fitbit2garmin-debug-activity-type skill, or query the `activity` table "
                "WHERE reconciliation_notes IS NOT NULL), then re-run with --allow-unmapped, "
                "or use --sample first.",
                err=True,
            )
            raise SystemExit(1)

        conn = pipeline.open_db(db_path)
        if sample_n:
            uids = pipeline.select_validation_sample(conn, sample_n)
            click.echo(f"\n🧪 Validation-batch mode: generating {len(uids)} representative activities")
        else:
            uids = [r["activity_uid"] for r in conn.execute("SELECT activity_uid FROM activity").fetchall()]
            click.echo(f"\n🏃 Generating output for all {len(uids)} activities")

        results = pipeline.generate_activity_outputs(conn, uids, output_path, format_set)
        conn.close()

        click.echo(f"\n✅ Wrote {results['written']} activities to {output_path} (formats: {', '.join(sorted(format_set))})")
        if "fit" in format_set:
            click.echo(f"   GPS points written: {results['points_written']} (skipped: {results['points_skipped']})")
        if results["errors"]:
            click.echo(f"   ⚠️  {len(results['errors'])} activities failed to generate:", err=True)
            for err in results["errors"][:10]:
                click.echo(f"      {err['activity_uid']}: {err['error']}", err=True)

        click.echo("\n🎯 Next steps:")
        if sample_n:
            click.echo(f"1. Drag the files in {output_path}/fit into Garmin Connect's Import Data page")
            click.echo("2. Confirm activity type, GPS track, duration, and HR match the source data")
            click.echo(f"   (query: sqlite3 {db_path} \"SELECT * FROM activity WHERE activity_uid='...'\")")
            click.echo("3. Once confirmed, re-run without --sample for the full history")
        else:
            click.echo("1. Upload the .fit files to Garmin Connect (Import Data menu or drag-and-drop)")
            click.echo("2. FIT carries full sport-type fidelity; TCX/GPX are secondary fallbacks")
            click.echo("3. If uploading many files, run `fitbit2garmin batch-output` first --")
            click.echo("   Garmin's importer is unreliable with very large single-batch uploads")

    except FileNotFoundError as e:
        click.echo(f"❌ Error: {e}", err=True)
        raise SystemExit(1)
    except Exception as e:
        click.echo(f"❌ Unexpected error: {e}", err=True)
        logger.exception("Unexpected error during conversion")
        raise SystemExit(1)


@cli.command()
@click.argument("takeout_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--db", "db_path", type=click.Path(path_type=Path), default=None,
    help=f"SQLite staging DB path (default: <takeout_path>/{DEFAULT_DB_FILENAME})",
)
def ingest(takeout_path, db_path):
    """Parse a Takeout export's Activities data into the SQLite staging database.

    Idempotent: unchanged files are skipped on re-run via content-hash tracking.
    Useful standalone when you want to inspect/query the staging DB (e.g. via the
    fitbit2garmin-debug-activity-type skill) without generating output files yet.
    """
    db_path = db_path or (Path(takeout_path) / DEFAULT_DB_FILENAME)
    click.echo(f"Ingesting {takeout_path} -> {db_path}")
    try:
        counts = pipeline.run_ingest(Path(takeout_path), db_path)
    except FileNotFoundError as e:
        click.echo(f"❌ {e}", err=True)
        raise SystemExit(1)

    for source, n in counts.items():
        click.echo(f"  {source}: {n} new rows ingested")

    summary = pipeline.ingest_summary(db_path)
    click.echo("\nStaging DB totals:")
    click.echo(f"  raw_user_exercise: {summary['raw_user_exercise']}")
    click.echo(f"  raw_exercise_json: {summary['raw_exercise_json']}")
    click.echo(f"  gps_point:         {summary['gps_point']} ({summary['gps_point_by_source']})")
    click.echo(f"  tcx files with GPS: {summary['tcx_files']}")


@cli.command()
@click.argument("db_path", type=click.Path(exists=True, path_type=Path))
def reconcile(db_path):
    """Rebuild the canonical activity table from ingested data.

    Idempotent: safe to re-run after a matcher/mapping-table code change without
    re-ingesting. Requires `fitbit2garmin ingest` to have been run first.
    """
    stats = pipeline.run_reconcile(Path(db_path))
    click.echo(f"Total activities: {stats['total']} (skipped: {stats['skipped']})")
    click.echo(
        f"  match: exact={stats['match_time_exact']} fuzzy={stats['match_time_fuzzy']} "
        f"unmatched={stats['match_user_exercises_only']}"
    )
    click.echo(
        f"  gps: tcx-exact={stats['gps_exact']} windowed={stats['gps_windowed']} "
        f"no_data={stats['gps_flagged_no_data']} not_expected={stats['gps_not_expected']}"
    )
    click.echo(f"  unmapped activity types: {stats['unmapped_type']}")
    click.echo(f"  orphan exercise_json records (not in UserExercises): {stats['orphan_exercise_json_count']}")


@cli.command("export-weight")
@click.argument("takeout_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--db", "db_path", type=click.Path(path_type=Path), default=None,
    help=f"SQLite staging DB path (default: <takeout_path>/{DEFAULT_DB_FILENAME})",
)
@click.option(
    "--output-dir", "output_dir", type=click.Path(path_type=Path), default="./output",
    help="Output directory",
)
@click.option(
    "--format", "fmt", type=click.Choice(["fit", "csv", "both"]), default="fit",
    help="fit (default, primary -- confirmed working against a real Garmin Connect import), "
         "csv (Garmin's official Fitbit-import CSV -- secondary, needed real bug fixes to get "
         "this far and is not yet confirmed working), or both.",
)
@click.option(
    "--locale", type=click.Choice(["iso", "us", "eu"]), default="iso",
    help="CSV only. Date/number format: 'iso' (YYYY-MM-DD, decimal point -- confirmed correct, "
         "matches simonepri/fitbit2garmin's real working output), 'us'/'eu' as a fallback.",
)
@click.option(
    "--units", type=click.Choice(["imperial", "metric"]), default="imperial",
    help="CSV only. Weight unit to write (lbs or kg) -- FIT always writes kg per the FIT spec.",
)
@click.option(
    "--sample-days", type=int, default=None,
    help="Validation mode: only export the first N days of weight history, to test-import "
         "via Garmin Connect before exporting the full history.",
)
def export_weight(takeout_path, db_path, output_dir, fmt, locale, units, sample_days):
    """Export weight/BMI/body-fat to Garmin-importable file(s).

    FIT (weight_scale messages) is the primary path -- confirmed working against
    a real Garmin Connect account. The CSV path (Garmin's official "Import Data
    From Fitbit" feature) is kept as a secondary option; see PROGRESS.md Phase 2.
    """
    db_path = Path(db_path) if db_path else Path(takeout_path) / DEFAULT_DB_FILENAME
    output_dir = Path(output_dir)
    click.echo(f"📖 Ingesting weight data from {takeout_path}...")
    n_ingested = pipeline.run_ingest_weight(Path(takeout_path), db_path)
    click.echo(f"   {n_ingested} new weight entries ingested")

    if fmt in ("fit", "both"):
        # FIT has no per-entry date filtering hook (it's a single small file with
        # everything) -- --sample-days only limits the CSV path; the FIT file
        # always carries the full history, which is fine since it's confirmed
        # working and there's no reason to hold it back behind a sample check.
        fit_path, n_fit = pipeline.export_weight_fit(db_path, output_dir / "weight.fit")
        click.echo(f"✅ Wrote {n_fit} weight entries to {fit_path}")

    if fmt in ("csv", "both"):
        csv_path, n_csv = pipeline.export_weight_csv(
            db_path, output_dir / "weight_garmin_import.csv", locale=locale, units=units, sample_days=sample_days
        )
        click.echo(f"✅ Wrote {n_csv} weight entries to {csv_path} (locale={locale}, units={units})")
        if sample_days:
            click.echo("   CSV validation mode: test-import this small slice via Garmin Connect's")
            click.echo("   'Import Data From Fitbit' wizard before exporting the full history.")

    if fmt == "fit":
        click.echo("\n🎯 Next steps: drag weight.fit into Garmin Connect's Import Data page.")
    else:
        click.echo("\n⚠️  This format/unit combination has not been confirmed against a real Garmin import yet.")
        click.echo("   Strongly recommend running with --sample-days 30 first and checking Garmin Connect.")


@cli.command("export-monitoring")
@click.argument("takeout_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--db", "db_path", type=click.Path(path_type=Path), default=None,
    help=f"SQLite staging DB path (default: <takeout_path>/{DEFAULT_DB_FILENAME})",
)
@click.option("--output-dir", "output_dir", type=click.Path(path_type=Path), default="./output")
@click.option(
    "--locale", type=click.Choice(["iso", "us", "eu"]), default="iso",
    help="Daily-totals CSV date/number format -- see export-weight's --locale for the same caveats.",
)
@click.option("--units", type=click.Choice(["imperial", "metric"]), default="imperial")
@click.option(
    "--sample-days", type=int, default=None,
    help="Validation mode: only export the first N days of daily-totals data (steps/calories/"
         "distance/floors/active-minutes -> Garmin Intensity Minutes on import), to test-import "
         "via Garmin Connect before exporting the full history. Does not affect the CSV archive.",
)
@click.option(
    "--include-fit-monitoring", is_flag=True,
    help="Also generate sleep.fit/resting_hr.fit/spo2.fit/hrv.fit -- OFF by default. "
         "CONFIRMED PROBLEMATIC against a real Garmin Connect account: sleep.fit is rejected "
         "outright ('Register your device'), and resting_hr/spo2/hrv.fit upload successfully "
         "but each daily reading shows up as a separate fake zero-duration activity, polluting "
         "your real activity history. Only use this if you understand and accept that.",
)
def export_monitoring(takeout_path, db_path, output_dir, locale, units, sample_days, include_fit_monitoring):
    """Export sleep/HR/SpO2/HRV/daily-totals -- BEST-EFFORT, lowest priority.

    Default output is a personal-reference CSV archive (sleep/resting-HR/SpO2/
    HRV) -- NOT importable into Garmin Connect, but doesn't risk breaking
    anything either. The daily-totals CSV (steps/calories/distance/floors/
    active-minutes -- Garmin's own docs describe converting the active-minutes
    columns to "Intensity Minutes" on import) uses Garmin's official "Import
    Data From Fitbit" format and may actually import correctly, similar to the
    confirmed-working weight.fit path, but is itself not yet user-confirmed.
    See --include-fit-monitoring's help text before using it -- the FIT path
    for sleep/HR/SpO2/HRV was tried against a real account and confirmed to
    make things worse, not better.
    """
    db_path = Path(db_path) if db_path else Path(takeout_path) / DEFAULT_DB_FILENAME
    output_dir = Path(output_dir)

    click.echo(f"📖 Ingesting monitoring data from {takeout_path}...")
    click.echo("   (scope: daily-granularity only -- steps/calories/distance/floors/active-minutes")
    click.echo("   are aggregated to daily sums, not stored per-minute, to stay memory-proportionate)")
    counts = pipeline.run_ingest_monitoring(Path(takeout_path), db_path)
    for k, n in counts.items():
        if n:
            click.echo(f"   {k}: {n} new rows")

    csv_path, n_days = pipeline.export_daily_totals_csv(
        db_path, output_dir / "daily_totals_garmin_import.csv", locale=locale, units=units, sample_days=sample_days
    )
    click.echo(f"\n✅ Wrote {n_days} days to {csv_path} (Garmin's official CSV import format)")
    if sample_days:
        click.echo(f"   Validation mode: first {sample_days} days only. Test-import this via Garmin")
        click.echo("   Connect's 'Import Data From Fitbit' wizard before re-running without --sample-days.")

    archive_results = pipeline.export_monitoring_archive(db_path, output_dir)
    for name, (path, n) in archive_results.items():
        if n:
            click.echo(f"✅ Wrote {n} {name} records to {path.name} (personal reference only, not Garmin-importable)")

    if include_fit_monitoring:
        click.echo("\n⚠️  Generating FIT monitoring files despite confirmed real problems (see --help):")
        results = pipeline.export_monitoring_fit(db_path, output_dir)
        for name, r in results.items():
            if r["count"]:
                click.echo(f"   Wrote {r['count']} {name} records to {', '.join(p.name for p in r['paths'])}")
        click.echo("   sleep.fit is expected to be REJECTED by Garmin Connect ('Register your device').")
        click.echo("   resting_hr/spo2/hrv.fit will upload but appear as individual fake activities.")
    else:
        click.echo("\nℹ️  sleep/resting-HR/SpO2/HRV are CSV-only by default (personal reference, not")
        click.echo("   Garmin-importable) -- the FIT path was tried against a real account and confirmed")
        click.echo("   to make things worse, not better. See --help for --include-fit-monitoring.")


@cli.command("batch-output")
@click.argument("directory", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option(
    "--batch-size", type=int, default=100,
    help="Max files per batch folder. Garmin Connect's web importer is unreliable with very "
         "large single-batch uploads (confirmed against a real account) -- 50-100 is a safe range.",
)
def batch_output(directory, batch_size):
    """Split a folder of generated files into batch_NNN/ subfolders for upload.

    Run this on output/fit (or tcx/gpx) after `convert` if you have a large
    number of files -- upload one batch_NNN/ folder at a time to Garmin Connect,
    which avoids the failures large single-shot batches can trigger.
    """
    batch_dirs = pipeline.split_into_batches(Path(directory), batch_size)
    if not batch_dirs:
        click.echo(f"No files found directly in {directory} (already batched, or empty).")
        return
    click.echo(f"Split into {len(batch_dirs)} batches of up to {batch_size} files each:")
    for d in batch_dirs:
        n = len(list(d.iterdir()))
        click.echo(f"  {d} ({n} files)")
    click.echo("\nUpload one batch folder at a time to Garmin Connect's Import Data page.")


@cli.command()
def info():
    """Show an overview of what this tool does and how to use it."""
    click.echo(f"fitbit2garmin v{__version__}")
    click.echo("=" * 50)

    click.echo("\n📊 Data domains and their status:")
    click.echo("✅ Activities (convert)         -- primary deliverable, confirmed working")
    click.echo("✅ Weight/BMI/body-fat          -- confirmed working (FIT primary, CSV secondary)")
    click.echo("❌ Sleep/HR/SpO2/HRV via FIT      -- confirmed broken (rejected, or pollutes activity feed)")
    click.echo("⚠️  Daily-totals CSV              -- format confirmed, not yet tested against Garmin")

    click.echo("\n📄 Commands:")
    click.echo("  convert <takeout_dir>            Activities -> FIT/TCX/GPX")
    click.echo("  export-weight <takeout_dir>       Weight/BMI/body-fat -> FIT (+ optional CSV)")
    click.echo("  export-monitoring <takeout_dir>   Sleep/HR/SpO2/HRV -> CSV archive; daily-totals -> Garmin CSV")
    click.echo("  batch-output <dir>                Split a large output folder into upload-sized batches")
    click.echo("  ingest / reconcile                Lower-level steps, useful for debugging via SQLite")

    click.echo("\n🔧 Typical flow:")
    click.echo("1. fitbit2garmin convert path/to/Takeout --sample 15   (validate a small batch first)")
    click.echo("2. Upload output/fit/*.fit to Garmin Connect and confirm it looks right")
    click.echo("3. fitbit2garmin convert path/to/Takeout                (full run, no --sample)")
    click.echo("4. fitbit2garmin batch-output output/fit                (if uploading hundreds of files)")
    click.echo("5. fitbit2garmin export-weight path/to/Takeout")


def main():
    """Main entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
