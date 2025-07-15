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
    "--parallel/--no-parallel", default=True, help="Enable/disable parallel processing"
)
@click.option(
    "--max-workers",
    type=int,
    default=None,
    help="Maximum number of worker processes (default: auto)",
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

        # Parse Fitbit data
        click.echo("📖 Parsing Fitbit data...")
        parser = FitbitParser(
            takeout_path,
            enable_resume=resume,
            enable_parallel=parallel,
            max_workers=max_workers,
        )

        # Parse data with detailed progress reporting
        user_data = parser.parse_all_data()

        # Display summary
        click.echo(f"✅ Data parsed successfully:")
        click.echo(f"   • Activities: {user_data.total_activities}")
        click.echo(f"   • Sleep records: {user_data.total_sleep_records}")
        click.echo(f"   • Daily records: {user_data.total_daily_records}")
        click.echo(f"   • Heart rate records: {len(user_data.heart_rate_data)}")
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
            if user_data.activities:
                click.echo("🏃 Converting activities...")
                converter = DataConverter(output_path)

                activity_result = converter.batch_convert_activities(user_data)
                exported_files.extend(activity_result.get("tcx_files", []))
                exported_files.extend(activity_result.get("gpx_files", []))
                exported_files.extend(activity_result.get("fit_files", []))
            else:
                click.echo("ℹ️  No activities found to convert")

        # FIT exports are now handled in the activity conversion above

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

        # Instructions
        click.echo("\n🎯 Next steps:")
        click.echo(
            "1. For daily data: Upload CSV files to Garmin Connect web interface"
        )
        click.echo(
            "2. For activities: Upload FIT files (recommended), TCX, or GPX files to Garmin Connect"
        )
        click.echo("3. FIT files provide the best compatibility and smallest file size")
        click.echo(
            "4. Check the garmin_connect_import.csv file for bulk import compatibility"
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

        for activity_type, activities in sorted(type_groups.items()):
            click.echo(f"\n🏷️  {activity_type.upper()} ({len(activities)} activities):")

            # Show examples with full details
            for i, activity in enumerate(activities[:5]):
                type_id_str = (
                    f" [ID: {activity.activity_type_id}]"
                    if activity.activity_type_id
                    else " [No ID]"
                )
                click.echo(f"   {i+1}. {activity.activity_name}{type_id_str}")
                click.echo(f"      Original: {activity.original_activity_name}")
                from .converter import DataConverter

                converter = DataConverter("/tmp")
                click.echo(
                    f"      TCX Sport: {converter._map_activity_type_to_tcx(activity.activity_type)}"
                )

            if len(activities) > 5:
                click.echo(f"   ... and {len(activities) - 5} more")

        click.echo(f"\n🎯 Recommendations:")
        if "other" in type_groups:
            click.echo(
                f"• {len(type_groups['other'])} activities mapped to 'other' - these will appear as 'Other' in Garmin Connect"
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


def main():
    """Main entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
