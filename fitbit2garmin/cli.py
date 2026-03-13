"""
Command-line interface for the Fitbit to Garmin migration tool.
"""

import click
import logging
from pathlib import Path
from typing import Optional
import sys
from tqdm import tqdm

from . import __version__
from .parser import FitbitParser
from .converter import DataConverter
from .exporter import GarminExporter

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
    "--output-dir",
    "-o",
    type=click.Path(path_type=Path),
    default="./output",
    help="Output directory for converted files",
)
@click.option(
    "--format",
    "-f",
    multiple=True,
    type=click.Choice(["csv", "tcx", "gpx", "fit", "all"]),
    default=["all"],
    help="Export formats (can specify multiple)",
)
@click.option(
    "--activities-only", is_flag=True, help="Export only activities (no daily metrics)"
)
@click.option(
    "--daily-only", is_flag=True, help="Export only daily metrics (no activities)"
)
@click.option(
    "--resume/--no-resume",
    default=True,
    help="Enable/disable resume capability for interrupted conversions",
)
@click.option(
    "--parallel/--no-parallel",
    default=True,
    help="Enable/disable parallel processing (disable if experiencing memory issues)",
)
@click.option(
    "--max-workers",
    type=int,
    default=None,
    help="Maximum number of worker processes (default: auto)",
)
@click.option(
    "--memory-limit-mb",
    type=int,
    default=None,
    help="Memory limit in MB before parallel processing scales back (default: 75%% of available RAM)",
)
@click.option("--clear-cache", is_flag=True, help="Clear cached data and start fresh")
def convert(
    takeout_path,
    output_dir,
    format,
    activities_only,
    daily_only,
    resume,
    parallel,
    max_workers,
    memory_limit_mb,
    clear_cache,
):
    """Convert Fitbit Google Takeout data to Garmin formats.

    TAKEOUT_PATH: Path to extracted Google Takeout folder
    """
    try:
        # Validate conflicting options
        if activities_only and daily_only:
            click.echo(
                "Error: Cannot specify both --activities-only and --daily-only",
                err=True,
            )
            sys.exit(1)

        # Initialize components
        click.echo(f"🔄 Initializing Fitbit to Garmin converter v{__version__}")
        click.echo(f"📁 Input: {takeout_path}")
        click.echo(f"📤 Output: {output_dir}")

        # Initialize output directory and resume manager
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Handle cache clearing
        if clear_cache:
            click.echo("🗑️  Clearing cache...")
            from .utils import ResumeManager

            resume_manager = ResumeManager(output_path)
            resume_manager.clear_cache()

        # Display configuration
        if resume:
            click.echo("💾 Resume capability: Enabled")
        if parallel:
            workers_text = (
                f" (max workers: {max_workers})" if max_workers else " (auto)"
            )
            click.echo(f"🚀 Parallel processing: Enabled{workers_text}")
        if memory_limit_mb:
            click.echo(f"🧠 Memory limit: {memory_limit_mb} MB")
        else:
            try:
                import psutil
                available_mb = psutil.virtual_memory().available // (1024 * 1024)
                effective_mb = max(1024, int(available_mb * 0.75))
                click.echo(f"🧠 Memory limit: {effective_mb} MB (75% of {available_mb} MB available)")
            except Exception:
                pass

        # Parse Fitbit data
        click.echo("📖 Parsing Fitbit data...")
        parser = FitbitParser(
            takeout_path,
            enable_resume=resume,
            enable_parallel=parallel,
            max_workers=max_workers,
            memory_limit_mb=memory_limit_mb,
        )

        # Parse data with detailed progress reporting
        user_data = parser.parse_all_data()

        # Display summary
        activities_with_gps = sum(1 for a in user_data.activities if a.gps_data)
        activities_with_hr = sum(1 for a in user_data.activities if a.average_heart_rate)
        activities_with_dist = sum(1 for a in user_data.activities if a.distance)
        click.echo(f"✅ Data parsed successfully:")
        click.echo(f"   • Activities: {user_data.total_activities}")
        if user_data.activities:
            click.echo(f"     - With GPS data: {activities_with_gps}")
            click.echo(f"     - With heart rate: {activities_with_hr}")
            click.echo(f"     - With distance: {activities_with_dist}")
            hr_days = len(user_data.heart_rate_daily_stats)
            click.echo(f"     - Intraday HR days available: {hr_days}")
        click.echo(f"   • Sleep records: {user_data.total_sleep_records}")
        click.echo(f"   • Daily records: {user_data.total_daily_records}")
        click.echo(f"   • Body composition: {len(user_data.body_composition)}")
        click.echo(f"   • HRV records: {len(user_data.heart_rate_variability)}")
        click.echo(f"   • Active Zone Minutes: {len(user_data.active_zone_minutes)}")
        click.echo(
            f"   • Date range: {user_data.date_range[0]} to {user_data.date_range[1]}"
        )

        # Show activity type breakdown for debugging
        if user_data.activities:
            activity_types = {}
            for activity in user_data.activities:
                activity_type = activity.activity_type.value
                if activity_type not in activity_types:
                    activity_types[activity_type] = []
                activity_types[activity_type].append(
                    {
                        "name": activity.activity_name,
                        "type_id": activity.activity_type_id,
                        "original_name": activity.original_activity_name,
                    }
                )

            click.echo(f"\n🔍 Activity type breakdown:")
            for activity_type, activities in sorted(activity_types.items()):
                click.echo(f"   • {activity_type}: {len(activities)} activities")
                # Show first few examples
                for i, activity in enumerate(activities[:3]):
                    type_id_str = (
                        f" (ID: {activity['type_id']})" if activity["type_id"] else ""
                    )
                    click.echo(f"     - {activity['name']}{type_id_str}")
                if len(activities) > 3:
                    click.echo(f"     ... and {len(activities) - 3} more")

        # Check if we have data to convert
        if not any(
            [user_data.activities, user_data.daily_metrics, user_data.sleep_data]
        ):
            click.echo(
                "⚠️  No data found to convert. Please check your Takeout path.",
                err=True,
            )
            sys.exit(1)

        # Determine what to export
        export_formats = format if "all" not in format else ["csv", "tcx", "gpx", "fit"]

        # Export data
        click.echo("🔄 Converting data...")

        exported_files = []

        # CSV exports (daily data)
        if "csv" in export_formats and not activities_only:
            click.echo("📊 Exporting CSV files...")
            exporter = GarminExporter(output_path)

            csv_result = exporter.export_all_data(user_data)
            exported_files.extend(csv_result.get("csv", []))

            # Create Garmin Connect import file
            import_result = exporter.export_garmin_import_ready(user_data)
            if import_result.get("garmin_import"):
                click.echo(
                    f"📋 Created Garmin Connect import file: {import_result['garmin_import']}"
                )

        # Activity exports (TCX/GPX/FIT)
        if any(f in export_formats for f in ["tcx", "gpx", "fit"]) and not daily_only:
            # Warn if GPS data is missing from export (tcxLink URLs present but no local TCX)
            from .gps_fetcher import collect_gps_activities, _find_fitbit_path
            _fb_path = _find_fitbit_path(Path(takeout_path))
            if _fb_path:
                _gps_acts = collect_gps_activities(_fb_path)
                _activities_dir = _fb_path / "Activities"
                _local_tcx = (
                    len(list(_activities_dir.glob("*.tcx")))
                    if _activities_dir.exists() else 0
                )
                _gps_with_local = sum(
                    1 for a in user_data.activities if a.gps_data
                )
                if _gps_acts and _local_tcx == 0:
                    click.echo(
                        f"\n⚠️  GPS data not included in your Takeout export.\n"
                        f"   {len(_gps_acts)} activities have GPS data on Fitbit's servers\n"
                        f"   but it was NOT downloaded as part of the Takeout.\n"
                        f"\n"
                        f"   To get GPS in your FIT files:\n"
                        f"   1. Get a Fitbit access token (see: fitbit2garmin fetch-gps --help)\n"
                        f"   2. Run: fitbit2garmin fetch-gps {takeout_path} --token <your_token>\n"
                        f"   3. Re-run this convert command.\n"
                    )

            # Pass the HR data directory so intraday HR can be embedded in FIT files
            hr_data_dir = None
            if "global_export" in parser.data_directories:
                hr_data_dir = parser.data_directories["global_export"]
            converter = DataConverter(output_path, hr_data_dir=hr_data_dir)

            if user_data.activities:
                click.echo("🏃 Converting activities...")
                activity_formats = [f for f in export_formats if f in ("tcx", "gpx", "fit")]
                activity_result = converter.batch_convert_activities(user_data, formats=activity_formats)
                exported_files.extend(activity_result.get("tcx_files", []))
                exported_files.extend(activity_result.get("gpx_files", []))
                exported_files.extend(activity_result.get("fit_files", []))
            else:
                click.echo("ℹ️  No activities found to convert")

            # Weight / body composition FIT file
            if "fit" in export_formats and user_data.body_composition:
                click.echo(
                    f"⚖️  Exporting {len(user_data.body_composition)} body composition"
                    " records to weight.fit..."
                )
                weight_fit = converter.convert_body_composition_to_fit(
                    user_data.body_composition
                )
                if weight_fit:
                    exported_files.append(weight_fit)
                    click.echo(f"   ✅ weight.fit written → import into Garmin Connect")

            # Daily steps / distance / calories FIT file
            if "fit" in export_formats and user_data.daily_metrics:
                steps_count = sum(
                    1 for d in user_data.daily_metrics
                    if d.steps is not None and d.steps > 0
                )
                if steps_count:
                    click.echo(
                        f"👟 Exporting {steps_count} days of step data to monitoring.fit..."
                    )
                    steps_fit = converter.convert_daily_steps_to_fit(user_data.daily_metrics)
                    if steps_fit:
                        exported_files.append(steps_fit)
                        click.echo(f"   ✅ monitoring.fit written")

            # Sleep FIT file
            if "fit" in export_formats and user_data.sleep_data:
                click.echo(
                    f"😴 Exporting {len(user_data.sleep_data)} sleep records to sleep.fit..."
                )
                sleep_fit = converter.convert_sleep_to_fit(user_data.sleep_data)
                if sleep_fit:
                    exported_files.append(sleep_fit)
                    click.echo(f"   ✅ sleep.fit written")

            # SpO2 FIT file
            if "fit" in export_formats and user_data.spo2_data:
                valid_spo2 = sum(1 for e in user_data.spo2_data if e.spo2_percentage is not None)
                if valid_spo2:
                    click.echo(
                        f"🩸 Exporting {valid_spo2} SpO2 records to spo2.fit..."
                    )
                    spo2_fit = converter.convert_spo2_to_fit(user_data.spo2_data)
                    if spo2_fit:
                        exported_files.append(spo2_fit)
                        click.echo(f"   ✅ spo2.fit written")

            # HRV FIT file
            if "fit" in export_formats and user_data.heart_rate_variability:
                valid_hrv = sum(
                    1 for e in user_data.heart_rate_variability
                    if e.rmssd is not None and e.rmssd > 0
                )
                if valid_hrv:
                    click.echo(
                        f"💓 Exporting {valid_hrv} HRV records to hrv.fit..."
                    )
                    hrv_fit = converter.convert_hrv_to_fit(user_data.heart_rate_variability)
                    if hrv_fit:
                        exported_files.append(hrv_fit)
                        click.echo(f"   ✅ hrv.fit written")

        # FIT exports are now handled in the activity/weight/steps/sleep/spo2/hrv conversion above

        # Summary
        click.echo(f"✅ Conversion completed!")
        click.echo(f"📁 {len(exported_files)} files exported to: {output_path}")

        # Show file list if not too many
        if len(exported_files) <= 10:
            click.echo("📄 Generated files:")
            for file in exported_files:
                click.echo(f"   • {Path(file).name}")
        else:
            click.echo(f"📄 {len(exported_files)} files generated (too many to list)")

        # Show FIT vs TCX guidance for sport-specific activities
        if user_data.activities:
            from .converter import DataConverter as _DC
            _conv = _DC("/tmp")
            fit_only = [
                a for a in user_data.activities
                if _conv._map_activity_type_to_tcx(a.activity_type) == "Other"
                and _conv._garmin_sport_name(a.activity_type) != "Generic"
            ]
            if fit_only:
                sport_counts: dict = {}
                for a in fit_only:
                    label = f"{a.activity_type.value} → {_conv._garmin_sport_name(a.activity_type)}"
                    sport_counts[label] = sport_counts.get(label, 0) + 1
                click.echo(
                    f"\n⚠️  {len(fit_only)} activities need FIT format for correct sport type in Garmin Connect:"
                )
                for label, cnt in sorted(sport_counts.items()):
                    click.echo(f"   • {label}  ({cnt} activities)")
                click.echo(
                    "   TCX format only supports Running/Walking/Biking/Swimming/Other."
                )
                click.echo(
                    "   Import the .fit files (not .tcx) for these activities."
                )

        # Instructions
        click.echo("\n🎯 Next steps:")
        click.echo(
            "1. Upload FIT files to Garmin Connect (Import Data menu or drag-and-drop)"
        )
        click.echo(
            "2. FIT files include GPS, heart rate, and correct sport type for all 30+ activity types"
        )
        click.echo(
            "3. If a FIT file fails to import, try the matching TCX or GPX file as a fallback"
        )
        click.echo(
            "4. CSV files are a personal data archive — Garmin Connect cannot import daily metrics (steps, sleep, HR, weight)"
        )

    except FileNotFoundError as e:
        click.echo(f"❌ Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"❌ Unexpected error: {e}", err=True)
        logger.exception("Unexpected error during conversion")
        sys.exit(1)


@cli.command()
@click.argument("takeout_path", type=click.Path(exists=True, path_type=Path))
def analyze(takeout_path):
    """Analyze Fitbit data without converting.

    TAKEOUT_PATH: Path to extracted Google Takeout folder
    """
    try:
        click.echo("🔍 Analyzing Fitbit data...")

        parser = FitbitParser(takeout_path)
        user_data = parser.parse_all_data()

        # Display detailed analysis
        click.echo("\n📊 Data Analysis:")
        click.echo("=" * 50)

        # Date range
        start_date, end_date = user_data.date_range
        total_days = (end_date - start_date).days + 1
        click.echo(f"📅 Date range: {start_date} to {end_date} ({total_days} days)")

        # Activities
        click.echo(f"\n🏃 Activities: {user_data.total_activities}")
        if user_data.activities:
            activity_types = {}
            total_distance = 0
            total_calories = 0

            for activity in user_data.activities:
                activity_types[activity.activity_type.value] = (
                    activity_types.get(activity.activity_type.value, 0) + 1
                )
                if activity.distance:
                    total_distance += activity.distance
                if activity.calories:
                    total_calories += activity.calories

            click.echo(f"   • Total distance: {total_distance:.1f} km")
            click.echo(f"   • Total calories: {total_calories:,}")
            click.echo("   • Activity types:")
            for activity_type, count in sorted(activity_types.items()):
                click.echo(f"     - {activity_type}: {count}")

        # Sleep data
        click.echo(f"\n😴 Sleep records: {user_data.total_sleep_records}")
        if user_data.sleep_data:
            total_sleep_hours = sum(
                sleep.total_sleep_hours for sleep in user_data.sleep_data
            )
            avg_sleep_hours = total_sleep_hours / len(user_data.sleep_data)
            click.echo(f"   • Average sleep: {avg_sleep_hours:.1f} hours/night")
            click.echo(f"   • Total sleep tracked: {total_sleep_hours:.1f} hours")

        # Daily metrics
        click.echo(f"\n📈 Daily metrics: {user_data.total_daily_records}")
        if user_data.daily_metrics:
            total_steps = sum(m.steps for m in user_data.daily_metrics if m.steps)
            avg_steps = (
                total_steps / len([m for m in user_data.daily_metrics if m.steps])
                if total_steps > 0
                else 0
            )
            click.echo(f"   • Total steps: {total_steps:,}")
            click.echo(f"   • Average steps: {avg_steps:.0f}/day")

        # Heart rate data
        click.echo(f"\n❤️  Heart rate records: {len(user_data.heart_rate_data)}")
        if user_data.heart_rate_data:
            avg_hr = sum(hr.bpm for hr in user_data.heart_rate_data) / len(
                user_data.heart_rate_data
            )
            click.echo(f"   • Average heart rate: {avg_hr:.0f} bpm")

        # Data quality assessment
        click.echo(f"\n🔍 Data Quality Assessment:")
        click.echo(
            f"   • Days with activities: {len(set(a.start_time.date() for a in user_data.activities))}"
        )
        click.echo(
            f"   • Days with sleep data: {len(set(s.date_of_sleep for s in user_data.sleep_data))}"
        )
        click.echo(
            f"   • Days with daily metrics: {len(set(m.date for m in user_data.daily_metrics))}"
        )

        # Recommendations
        click.echo(f"\n💡 Recommendations:")
        if user_data.activities:
            gps_activities = len([a for a in user_data.activities if a.gps_data])
            click.echo(
                f"   • {gps_activities} activities have GPS data (suitable for GPX export)"
            )

        if user_data.daily_metrics:
            click.echo(f"   • Daily metrics are good for CSV import to Garmin Connect")

        if user_data.sleep_data:
            click.echo(f"   • Sleep data can be imported as daily summaries")

    except Exception as e:
        click.echo(f"❌ Error analyzing data: {e}", err=True)
        logger.exception("Error during analysis")
        sys.exit(1)


@cli.command()
@click.argument("takeout_path", type=click.Path(exists=True, path_type=Path))
def debug_activities(takeout_path):
    """Debug activity type detection and mapping.

    TAKEOUT_PATH: Path to extracted Google Takeout folder
    """
    try:
        click.echo("🔍 Analyzing activity type detection...")

        parser = FitbitParser(takeout_path)
        user_data = parser.parse_all_data()

        if not user_data.activities:
            click.echo("❌ No activities found in the data.")
            return

        # Group activities by detected type
        type_groups = {}
        for activity in user_data.activities:
            activity_type = activity.activity_type.value
            if activity_type not in type_groups:
                type_groups[activity_type] = []
            type_groups[activity_type].append(activity)

        # Show detailed breakdown
        click.echo(
            f"\n📊 Activity Type Analysis ({len(user_data.activities)} total activities):"
        )
        click.echo("=" * 60)

        from .converter import DataConverter
        converter = DataConverter("/tmp")

        # Track activities where TCX shows "Other" but FIT has a proper sport
        fit_only_sports = []

        for activity_type, activities in sorted(type_groups.items()):
            # Determine TCX and FIT sport for this type
            sample = activities[0]
            tcx_sport = converter._map_activity_type_to_tcx(sample.activity_type)
            fit_sport = converter._garmin_sport_name(sample.activity_type)
            needs_fit = tcx_sport == "Other" and fit_sport != "Generic"

            sport_label = f"TCX={tcx_sport}  |  FIT={fit_sport}"
            flag = "  ⚠️  use .fit file" if needs_fit else ""
            click.echo(f"\n🏷️  {activity_type.upper()} ({len(activities)} activities)  [{sport_label}]{flag}:")

            if needs_fit:
                fit_only_sports.append((activity_type, fit_sport, len(activities)))

            # Show examples with full details
            for i, activity in enumerate(activities[:5]):
                type_id_str = (
                    f" [ID: {activity.activity_type_id}]"
                    if activity.activity_type_id
                    else " [No ID]"
                )
                click.echo(f"   {i+1}. {activity.activity_name}{type_id_str}")
                if activity.original_activity_name and activity.original_activity_name != activity.activity_name:
                    click.echo(f"      Original: {activity.original_activity_name}")

            if len(activities) > 5:
                click.echo(f"   ... and {len(activities) - 5} more")

        click.echo(f"\n🎯 Recommendations:")

        if fit_only_sports:
            click.echo(
                f"\n⚠️  {len(fit_only_sports)} activity type(s) require FIT format for correct sport in Garmin Connect:"
            )
            click.echo(
                "   TCX format only supports Running/Walking/Biking/Swimming/Other."
            )
            click.echo(
                "   The following sports appear as 'Other' in TCX but are correct in .fit files:"
            )
            for act_type, garmin_sport, count in sorted(fit_only_sports):
                click.echo(f"   • {act_type} → {garmin_sport}  ({count} activities)")
            click.echo(
                "\n   ACTION: Import the .fit files (not .tcx) to Garmin Connect for these activities."
            )

        if "other" in type_groups:
            click.echo(
                f"\n• {len(type_groups['other'])} activities mapped to 'other' - these will appear as 'Other' in Garmin Connect"
            )

        # Check for common unmapped activity type IDs
        unmapped_ids = set()
        for activity in user_data.activities:
            if activity.activity_type == "other" and activity.activity_type_id:
                unmapped_ids.add(activity.activity_type_id)

        if unmapped_ids:
            click.echo(
                f"• Found {len(unmapped_ids)} unmapped Fitbit activity type IDs: {sorted(unmapped_ids)}"
            )
            click.echo("  Consider adding these to the fitbit_id_mapping in parser.py")

    except Exception as e:
        click.echo(f"❌ Error analyzing activities: {e}", err=True)
        logger.exception("Error during activity analysis")
        sys.exit(1)


@cli.command()
def info():
    """Show information about supported data types and formats."""
    click.echo(f"Fitbit to Garmin Converter v{__version__}")
    click.echo("=" * 50)

    click.echo("\n📊 Supported Data Types:")
    click.echo("✅ Activities (runs, walks, workouts, etc.)")
    click.echo("✅ Daily metrics (steps, calories, distance)")
    click.echo("✅ Sleep data (duration, efficiency, stages)")
    click.echo("✅ Heart rate data (continuous monitoring)")
    click.echo("✅ Body composition (weight, BMI, body fat)")
    click.echo("⚠️  Heart rate variability (limited)")
    click.echo("⚠️  Stress data (limited)")
    click.echo("⚠️  Temperature data (limited)")

    click.echo("\n📄 Export Formats:")
    click.echo("✅ CSV - Daily metrics, compatible with Garmin Connect import")
    click.echo("✅ TCX - Activities with heart rate and GPS data")
    click.echo("✅ GPX - GPS tracks for activities")
    click.echo("✅ FIT - Native Garmin format with comprehensive data (recommended)")

    click.echo("\n🔧 Usage Tips:")
    click.echo("1. Export your Fitbit data using Google Takeout")
    click.echo("2. Extract the downloaded archive")
    click.echo("3. Run: fitbit2garmin convert path/to/Takeout")
    click.echo("4. Upload generated files to Garmin Connect")
    click.echo(
        "5. For activity type debugging: fitbit2garmin debug-activities path/to/Takeout"
    )


@cli.command("fetch-gps")
@click.argument("takeout_path", type=click.Path(exists=True))
@click.option(
    "--token",
    required=True,
    envvar="FITBIT_TOKEN",
    help=(
        "Fitbit API access token (Bearer token). "
        "Can also be set via the FITBIT_TOKEN environment variable."
    ),
)
@click.option(
    "--output-dir",
    type=click.Path(),
    default=None,
    help="Directory to save downloaded TCX files (default: <Fitbit>/Activities/).",
)
def fetch_gps(takeout_path, token, output_dir):
    """Download GPS tracks from Fitbit API for activities missing GPS data.

    \b
    Fitbit's Google Takeout does NOT include GPS track data — it only stores
    a tcxLink URL per GPS activity. This command fetches the actual GPS tracks
    from the Fitbit API and saves them as local TCX files so that 'convert'
    can embed GPS in your FIT files.

    \b
    HOW TO GET A FITBIT ACCESS TOKEN (one-time setup):
      1. Go to https://dev.fitbit.com/apps/new
         - Application Type: Personal
         - Redirect URL: https://localhost
      2. Note your Client ID.
      3. Open in a browser (replace YOUR_CLIENT_ID):
         https://www.fitbit.com/oauth2/authorize?response_type=token
           &client_id=YOUR_CLIENT_ID&redirect_uri=https%3A%2F%2Flocalhost
           &scope=activity%20location&expires_in=604800
         NOTE: Both 'activity' AND 'location' scopes are required for GPS data.
      4. Approve, then copy the access_token from the redirect URL.
      5. Run this command with --token <token>.

    \b
    After running, re-run 'convert' to generate FIT files with GPS embedded.
    """
    from .gps_fetcher import fetch_gps_files, collect_gps_activities, _find_fitbit_path

    takeout = Path(takeout_path)
    out_dir = Path(output_dir) if output_dir else None

    try:
        fitbit_path = _find_fitbit_path(takeout)
        if fitbit_path is None:
            click.echo(f"❌ Fitbit data not found under {takeout}", err=True)
            raise SystemExit(1)

        gps_acts = collect_gps_activities(fitbit_path)
        if not gps_acts:
            click.echo("ℹ️  No GPS activities found in your Takeout export.")
            return

        click.echo(
            f"📍 Found {len(gps_acts)} activities with GPS data to download."
        )

        if out_dir is None:
            out_dir = fitbit_path / "Activities"
        click.echo(f"   Saving TCX files to: {out_dir}")

        downloaded, failed = fetch_gps_files(
            takeout_path=takeout,
            token=token,
            output_activities_dir=out_dir,
        )

        click.echo(f"\n✅ Downloaded: {downloaded}  |  Failed: {failed}")
        if downloaded > 0:
            click.echo(
                "\nNext step: re-run 'fitbit2garmin convert' to regenerate FIT files "
                "with GPS embedded."
            )
        if failed > 0:
            click.echo(
                f"⚠️  {failed} downloads failed. Check --verbose for details. "
                "Common causes: expired token, activity deleted from Fitbit."
            )

    except RuntimeError as e:
        click.echo(f"❌ {e}", err=True)
        raise SystemExit(1)
    except Exception as e:
        click.echo(f"❌ Unexpected error: {e}", err=True)
        if logger.isEnabledFor(logging.DEBUG):
            import traceback
            traceback.print_exc()
        raise SystemExit(1)


def main():
    """Main entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
