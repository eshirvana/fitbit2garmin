"""
Export Fitbit data to Garmin Connect compatible formats.
"""

import csv
import logging
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
import pandas as pd

from .models import (
    FitbitUserData, ActivityData, SleepData, DailyMetrics, BodyComposition,
    HeartRateData, HeartRateVariability, StressData, TemperatureData,
    SpO2Data, ActiveZoneMinutes
)

logger = logging.getLogger(__name__)


class GarminExporter:
    """Export Fitbit data to Garmin Connect compatible formats."""
    
    def __init__(self, output_dir: Union[str, Path]):
        """Initialize exporter with output directory."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Initialized Garmin exporter with output directory: {self.output_dir}")
    
    def export_all_data(self, user_data: FitbitUserData) -> Dict[str, List[str]]:
        """Export all user data to Garmin compatible formats."""
        logger.info("Starting export of all data to Garmin formats")
        
        exported_files = {
            'csv': [],
            'tcx': [],
            'gpx': [],
            'fit': []
        }
        
        # Export daily metrics to CSV
        if user_data.daily_metrics:
            print("  📊 Exporting daily metrics...")
            csv_files = self._export_daily_metrics_csv(user_data.daily_metrics)
            exported_files['csv'].extend(csv_files)
        
        # Export sleep data to CSV
        if user_data.sleep_data:
            print("  😴 Exporting sleep data...")
            sleep_file = self._export_sleep_data_csv(user_data.sleep_data)
            exported_files['csv'].append(sleep_file)
        
        # Export body composition to CSV
        if user_data.body_composition:
            print("  🏋️ Exporting body composition...")
            body_file = self._export_body_composition_csv(user_data.body_composition)
            exported_files['csv'].append(body_file)
        
        # Export heart rate data to CSV
        if user_data.heart_rate_data:
            print("  ❤️ Exporting heart rate data...")
            hr_file = self._export_heart_rate_csv(user_data.heart_rate_data)
            exported_files['csv'].append(hr_file)
        
        # Export activities (placeholder for now)
        if user_data.activities:
            print("  🏃 Exporting activities summary...")
            activity_files = self._export_activities_summary_csv(user_data.activities)
            exported_files['csv'].extend(activity_files)
        
        logger.info(f"Exported {len(exported_files['csv'])} CSV files")
        print(f"  ✅ Created {len(exported_files['csv'])} CSV files")
        return exported_files
    
    def _export_daily_metrics_csv(self, daily_metrics: List[DailyMetrics]) -> List[str]:
        """Export daily metrics to CSV format compatible with Garmin."""
        if not daily_metrics:
            return []
        
        # Create separate CSV files for different metric types
        csv_files = []
        
        # Steps data
        steps_file = self.output_dir / "fitbit_steps.csv"
        steps_data = []
        for metric in daily_metrics:
            if metric.steps is not None:
                steps_data.append({
                    'Date': metric.date.strftime('%Y-%m-%d'),
                    'Steps': metric.steps
                })
        
        if steps_data:
            df = pd.DataFrame(steps_data)
            df.to_csv(steps_file, index=False)
            csv_files.append(str(steps_file))
            logger.info(f"Exported {len(steps_data)} steps records to {steps_file}")
        
        # Distance data
        distance_file = self.output_dir / "fitbit_distance.csv"
        distance_data = []
        for metric in daily_metrics:
            if metric.distance is not None:
                distance_data.append({
                    'Date': metric.date.strftime('%Y-%m-%d'),
                    'Distance (km)': metric.distance
                })
        
        if distance_data:
            df = pd.DataFrame(distance_data)
            df.to_csv(distance_file, index=False)
            csv_files.append(str(distance_file))
            logger.info(f"Exported {len(distance_data)} distance records to {distance_file}")
        
        # Calories data
        calories_file = self.output_dir / "fitbit_calories.csv"
        calories_data = []
        for metric in daily_metrics:
            if metric.calories_burned is not None:
                calories_data.append({
                    'Date': metric.date.strftime('%Y-%m-%d'),
                    'Calories Burned': metric.calories_burned,
                    'Calories BMR': metric.calories_bmr or 0
                })
        
        if calories_data:
            df = pd.DataFrame(calories_data)
            df.to_csv(calories_file, index=False)
            csv_files.append(str(calories_file))
            logger.info(f"Exported {len(calories_data)} calories records to {calories_file}")
        
        # Activity minutes data
        activity_file = self.output_dir / "fitbit_activity_minutes.csv"
        activity_data = []
        for metric in daily_metrics:
            if any([metric.sedentary_minutes, metric.lightly_active_minutes, 
                   metric.fairly_active_minutes, metric.very_active_minutes]):
                activity_data.append({
                    'Date': metric.date.strftime('%Y-%m-%d'),
                    'Sedentary Minutes': metric.sedentary_minutes or 0,
                    'Lightly Active Minutes': metric.lightly_active_minutes or 0,
                    'Fairly Active Minutes': metric.fairly_active_minutes or 0,
                    'Very Active Minutes': metric.very_active_minutes or 0,
                    'Active Minutes': metric.active_minutes or 0
                })
        
        if activity_data:
            df = pd.DataFrame(activity_data)
            df.to_csv(activity_file, index=False)
            csv_files.append(str(activity_file))
            logger.info(f"Exported {len(activity_data)} activity minutes records to {activity_file}")
        
        # Floors data
        floors_file = self.output_dir / "fitbit_floors.csv"
        floors_data = []
        for metric in daily_metrics:
            if metric.floors is not None:
                floors_data.append({
                    'Date': metric.date.strftime('%Y-%m-%d'),
                    'Floors': metric.floors
                })
        
        if floors_data:
            df = pd.DataFrame(floors_data)
            df.to_csv(floors_file, index=False)
            csv_files.append(str(floors_file))
            logger.info(f"Exported {len(floors_data)} floors records to {floors_file}")
        
        return csv_files
    
    def _export_sleep_data_csv(self, sleep_data: List[SleepData]) -> str:
        """Export sleep data to CSV format."""
        sleep_file = self.output_dir / "fitbit_sleep.csv"
        
        sleep_records = []
        for sleep in sleep_data:
            sleep_records.append({
                'Date': sleep.date_of_sleep.strftime('%Y-%m-%d'),
                'Start Time': sleep.start_time.strftime('%Y-%m-%d %H:%M:%S'),
                'End Time': sleep.end_time.strftime('%Y-%m-%d %H:%M:%S'),
                'Duration (hours)': sleep.total_sleep_hours,
                'Minutes Asleep': sleep.minutes_asleep or 0,
                'Minutes Awake': sleep.minutes_awake or 0,
                'Sleep Efficiency': sleep.efficiency or 0,
                'Minutes to Fall Asleep': sleep.minutes_to_fall_asleep or 0,
                'Minutes After Wakeup': sleep.minutes_after_wakeup or 0,
                'Time in Bed': sleep.time_in_bed or 0
            })
        
        df = pd.DataFrame(sleep_records)
        df.to_csv(sleep_file, index=False)
        logger.info(f"Exported {len(sleep_records)} sleep records to {sleep_file}")
        
        return str(sleep_file)
    
    def _export_body_composition_csv(self, body_data: List[BodyComposition]) -> str:
        """Export body composition data to CSV format."""
        body_file = self.output_dir / "fitbit_body_composition.csv"
        
        body_records = []
        for body in body_data:
            body_records.append({
                'Date': body.date.strftime('%Y-%m-%d'),
                'Weight (kg)': body.weight,
                'BMI': body.bmi,
                'Body Fat %': body.body_fat_percentage,
                'Lean Mass (kg)': body.lean_mass,
                'Muscle Mass (kg)': body.muscle_mass,
                'Bone Mass (kg)': body.bone_mass,
                'Water %': body.water_percentage
            })
        
        df = pd.DataFrame(body_records)
        df.to_csv(body_file, index=False)
        logger.info(f"Exported {len(body_records)} body composition records to {body_file}")
        
        return str(body_file)
    
    def _export_heart_rate_csv(self, heart_rate_data: List[HeartRateData]) -> str:
        """Export heart rate data to CSV format."""
        hr_file = self.output_dir / "fitbit_heart_rate.csv"
        
        # Group by date to get daily summaries
        daily_hr = {}
        for hr in heart_rate_data:
            date_key = hr.datetime.date()
            if date_key not in daily_hr:
                daily_hr[date_key] = {
                    'readings': [],
                    'high_confidence': []
                }
            
            daily_hr[date_key]['readings'].append(hr.bpm)
            if hr.confidence >= 2:  # High confidence readings
                daily_hr[date_key]['high_confidence'].append(hr.bpm)
        
        hr_records = []
        for date_key, data in daily_hr.items():
            readings = data['readings']
            high_conf = data['high_confidence'] if data['high_confidence'] else readings
            
            hr_records.append({
                'Date': date_key.strftime('%Y-%m-%d'),
                'Average Heart Rate': sum(readings) / len(readings) if readings else 0,
                'Min Heart Rate': min(readings) if readings else 0,
                'Max Heart Rate': max(readings) if readings else 0,
                'Resting Heart Rate': min(high_conf) if high_conf else 0,
                'Total Readings': len(readings),
                'High Confidence Readings': len(high_conf)
            })
        
        df = pd.DataFrame(hr_records)
        df.to_csv(hr_file, index=False)
        logger.info(f"Exported {len(hr_records)} daily heart rate summaries to {hr_file}")
        
        return str(hr_file)
    
    def _export_activities_summary_csv(self, activities: List[ActivityData]) -> List[str]:
        """Export activities summary to CSV format."""
        activities_file = self.output_dir / "fitbit_activities.csv"
        
        activity_records = []
        for activity in activities:
            activity_records.append({
                'Date': activity.start_time.strftime('%Y-%m-%d'),
                'Start Time': activity.start_time.strftime('%Y-%m-%d %H:%M:%S'),
                'Activity Name': activity.activity_name,
                'Activity Type': activity.activity_type.value,
                'Duration (minutes)': activity.duration_ms / 60000,
                'Calories': activity.calories or 0,
                'Distance (km)': activity.distance or 0,
                'Steps': activity.steps or 0,
                'Average Heart Rate': activity.average_heart_rate or 0,
                'Max Heart Rate': activity.max_heart_rate or 0,
                'Has GPS Data': bool(activity.gps_data),
                'Has TCX Data': bool(activity.tcx_data)
            })
        
        df = pd.DataFrame(activity_records)
        df.to_csv(activities_file, index=False)
        logger.info(f"Exported {len(activity_records)} activities to {activities_file}")
        
        return [str(activities_file)]
    
    def export_garmin_import_ready(self, user_data: FitbitUserData) -> Dict[str, str]:
        """Export data in formats ready for Garmin Connect import."""
        logger.info("Creating Garmin Connect import-ready files")
        
        # Create the master import file for Garmin Connect
        import_file = self.output_dir / "garmin_connect_import.csv"
        
        # Combine daily metrics into a single file format expected by Garmin
        import_data = []
        
        # Create a mapping of all daily data
        daily_data_map = {}
        
        # Add daily metrics
        for metric in user_data.daily_metrics:
            date_key = metric.date
            if date_key not in daily_data_map:
                daily_data_map[date_key] = {}
            
            daily_data_map[date_key].update({
                'Steps': metric.steps,
                'Distance': metric.distance,
                'Calories': metric.calories_burned,
                'Floors': metric.floors,
                'Active Minutes': metric.active_minutes
            })
        
        # Add sleep data
        for sleep in user_data.sleep_data:
            date_key = sleep.date_of_sleep
            if date_key not in daily_data_map:
                daily_data_map[date_key] = {}
            
            daily_data_map[date_key].update({
                'Sleep Duration (hours)': sleep.total_sleep_hours,
                'Sleep Efficiency': sleep.efficiency
            })
        
        # Add body composition data
        for body in user_data.body_composition:
            date_key = body.date
            if date_key not in daily_data_map:
                daily_data_map[date_key] = {}
            
            daily_data_map[date_key].update({
                'Weight': body.weight,
                'BMI': body.bmi,
                'Body Fat %': body.body_fat_percentage
            })
        
        # Convert to list format
        for date_key, data in sorted(daily_data_map.items()):
            record = {'Date': date_key.strftime('%Y-%m-%d')}
            record.update(data)
            import_data.append(record)
        
        # Export to CSV
        if import_data:
            df = pd.DataFrame(import_data)
            df.to_csv(import_file, index=False)
            logger.info(f"Created Garmin Connect import file: {import_file}")
        
        return {
            'garmin_import': str(import_file),
            'date_range': f"{user_data.date_range[0]} to {user_data.date_range[1]}",
            'total_records': len(import_data)
        }