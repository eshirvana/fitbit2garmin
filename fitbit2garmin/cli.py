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
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@click.group()
@click.version_option(version=__version__)
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
def cli(verbose):
    """Fitbit to Garmin data migration tool.
    
    Convert Fitbit Google Takeout data to Garmin Connect compatible formats.
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")


@cli.command()
@click.argument('takeout_path', type=click.Path(exists=True, path_type=Path))
@click.option('--output-dir', '-o', type=click.Path(path_type=Path), 
              default='./output', help='Output directory for converted files')
@click.option('--format', '-f', multiple=True, 
              type=click.Choice(['csv', 'tcx', 'gpx', 'fit', 'all']),
              default=['all'], help='Export formats (can specify multiple)')
@click.option('--activities-only', is_flag=True, 
              help='Export only activities (no daily metrics)')
@click.option('--daily-only', is_flag=True, 
              help='Export only daily metrics (no activities)')
def convert(takeout_path, output_dir, format, activities_only, daily_only):
    """Convert Fitbit Google Takeout data to Garmin formats.
    
    TAKEOUT_PATH: Path to extracted Google Takeout folder
    """
    try:
        # Validate conflicting options
        if activities_only and daily_only:
            click.echo("Error: Cannot specify both --activities-only and --daily-only", err=True)
            sys.exit(1)
        
        # Initialize components
        click.echo(f"🔄 Initializing Fitbit to Garmin converter v{__version__}")
        click.echo(f"📁 Input: {takeout_path}")
        click.echo(f"📤 Output: {output_dir}")
        
        # Parse Fitbit data
        click.echo("📖 Parsing Fitbit data...")
        parser = FitbitParser(takeout_path)
        
        # Parse data with detailed progress reporting
        user_data = parser.parse_all_data()
        
        # Display summary
        click.echo(f"✅ Data parsed successfully:")
        click.echo(f"   • Activities: {user_data.total_activities}")
        click.echo(f"   • Sleep records: {user_data.total_sleep_records}")
        click.echo(f"   • Daily records: {user_data.total_daily_records}")
        click.echo(f"   • Date range: {user_data.date_range[0]} to {user_data.date_range[1]}")
        
        # Check if we have data to convert
        if not any([user_data.activities, user_data.daily_metrics, user_data.sleep_data]):
            click.echo("⚠️  No data found to convert. Please check your Takeout path.", err=True)
            sys.exit(1)
        
        # Initialize output directory
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Determine what to export
        export_formats = format if 'all' not in format else ['csv', 'tcx', 'gpx']
        
        # Export data
        click.echo("🔄 Converting data...")
        
        exported_files = []
        
        # CSV exports (daily data)
        if 'csv' in export_formats and not activities_only:
            click.echo("📊 Exporting CSV files...")
            exporter = GarminExporter(output_path)
            
            csv_result = exporter.export_all_data(user_data)
            exported_files.extend(csv_result.get('csv', []))
            
            # Create Garmin Connect import file
            import_result = exporter.export_garmin_import_ready(user_data)
            if import_result.get('garmin_import'):
                click.echo(f"📋 Created Garmin Connect import file: {import_result['garmin_import']}")
        
        # Activity exports (TCX/GPX)
        if any(f in export_formats for f in ['tcx', 'gpx']) and not daily_only:
            if user_data.activities:
                click.echo("🏃 Converting activities...")
                converter = DataConverter(output_path)
                
                activity_result = converter.batch_convert_activities(user_data)
                exported_files.extend(activity_result.get('tcx_files', []))
                exported_files.extend(activity_result.get('gpx_files', []))
            else:
                click.echo("ℹ️  No activities found to convert")
        
        # FIT exports (placeholder)
        if 'fit' in export_formats:
            click.echo("⚠️  FIT export not yet implemented")
        
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
        click.echo("1. For daily data: Upload CSV files to Garmin Connect web interface")
        click.echo("2. For activities: Upload TCX/GPX files individually or use Garmin Connect API")
        click.echo("3. Check the garmin_connect_import.csv file for bulk import compatibility")
        
    except FileNotFoundError as e:
        click.echo(f"❌ Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"❌ Unexpected error: {e}", err=True)
        logger.exception("Unexpected error during conversion")
        sys.exit(1)


@cli.command()
@click.argument('takeout_path', type=click.Path(exists=True, path_type=Path))
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
                activity_types[activity.activity_type.value] = activity_types.get(activity.activity_type.value, 0) + 1
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
            total_sleep_hours = sum(sleep.total_sleep_hours for sleep in user_data.sleep_data)
            avg_sleep_hours = total_sleep_hours / len(user_data.sleep_data)
            click.echo(f"   • Average sleep: {avg_sleep_hours:.1f} hours/night")
            click.echo(f"   • Total sleep tracked: {total_sleep_hours:.1f} hours")
        
        # Daily metrics
        click.echo(f"\n📈 Daily metrics: {user_data.total_daily_records}")
        if user_data.daily_metrics:
            total_steps = sum(m.steps for m in user_data.daily_metrics if m.steps)
            avg_steps = total_steps / len([m for m in user_data.daily_metrics if m.steps]) if total_steps > 0 else 0
            click.echo(f"   • Total steps: {total_steps:,}")
            click.echo(f"   • Average steps: {avg_steps:.0f}/day")
        
        # Heart rate data
        click.echo(f"\n❤️  Heart rate records: {len(user_data.heart_rate_data)}")
        if user_data.heart_rate_data:
            avg_hr = sum(hr.bpm for hr in user_data.heart_rate_data) / len(user_data.heart_rate_data)
            click.echo(f"   • Average heart rate: {avg_hr:.0f} bpm")
        
        # Data quality assessment
        click.echo(f"\n🔍 Data Quality Assessment:")
        click.echo(f"   • Days with activities: {len(set(a.start_time.date() for a in user_data.activities))}")
        click.echo(f"   • Days with sleep data: {len(set(s.date_of_sleep for s in user_data.sleep_data))}")
        click.echo(f"   • Days with daily metrics: {len(set(m.date for m in user_data.daily_metrics))}")
        
        # Recommendations
        click.echo(f"\n💡 Recommendations:")
        if user_data.activities:
            gps_activities = len([a for a in user_data.activities if a.gps_data])
            click.echo(f"   • {gps_activities} activities have GPS data (suitable for GPX export)")
        
        if user_data.daily_metrics:
            click.echo(f"   • Daily metrics are good for CSV import to Garmin Connect")
        
        if user_data.sleep_data:
            click.echo(f"   • Sleep data can be imported as daily summaries")
        
    except Exception as e:
        click.echo(f"❌ Error analyzing data: {e}", err=True)
        logger.exception("Error during analysis")
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
    click.echo("⚠️  FIT - Advanced sensor data (planned)")
    
    click.echo("\n🔧 Usage Tips:")
    click.echo("1. Export your Fitbit data using Google Takeout")
    click.echo("2. Extract the downloaded archive")
    click.echo("3. Run: fitbit2garmin convert path/to/Takeout")
    click.echo("4. Upload generated files to Garmin Connect")


def main():
    """Main entry point for the CLI."""
    cli()


if __name__ == '__main__':
    main()