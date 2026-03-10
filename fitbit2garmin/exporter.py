"""
Export Fitbit data to Garmin Connect compatible formats.
"""

import csv
import logging
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
import pandas as pd

# Fitbit exports weight in pounds (lbs) for US-locale accounts and in kg for metric accounts.
# Since the JSON does not carry a unit field, we use this threshold as a heuristic:
# values above LBS_THRESHOLD are assumed to be in lbs and are converted to kg.
# Users should verify the output if their weight is close to this boundary.
_LBS_TO_KG = 0.453592
_LBS_THRESHOLD = 100.0  # kg; weights > 100 kg (220 lbs) are common only for lbs exports

from .models import (
    FitbitUserData,
    ActivityData,
    SleepData,
    DailyMetrics,
    BodyComposition,
    HeartRateData,
    HeartRateVariability,
    StressData,
    TemperatureData,
    SpO2Data,
    ActiveZoneMinutes,
)

logger = logging.getLogger(__name__)


class GarminExporter:
    """Export Fitbit data to Garmin Connect compatible formats."""

    def __init__(self, output_dir: Union[str, Path]):
        """Initialize exporter with output directory."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"Initialized Garmin exporter with output directory: {self.output_dir}"
        )

    def export_all_data(self, user_data: FitbitUserData) -> Dict[str, List[str]]:
        """Export all user data to Garmin compatible formats."""
        logger.info("Starting export of all data to Garmin formats")

        exported_files = {"csv": [], "tcx": [], "gpx": [], "fit": []}

        # Export daily metrics to CSV
        if user_data.daily_metrics:
            print("  📊 Exporting daily metrics...")
            csv_files = self._export_daily_metrics_csv(user_data.daily_metrics)
            exported_files["csv"].extend(csv_files)

        # Export sleep data to CSV
        if user_data.sleep_data:
            print("  😴 Exporting sleep data...")
            sleep_file = self._export_sleep_data_csv(user_data.sleep_data)
            exported_files["csv"].append(sleep_file)

        # Export body composition to CSV
        if user_data.body_composition:
            print("  🏋️ Exporting body composition...")
            body_file = self._export_body_composition_csv(user_data.body_composition)
            exported_files["csv"].append(body_file)

        # Export heart rate data to CSV
        if user_data.heart_rate_data:
            print("  ❤️ Exporting heart rate data...")
            hr_file = self._export_heart_rate_csv(user_data.heart_rate_data)
            exported_files["csv"].append(hr_file)

        # Export HRV data to CSV
        if user_data.heart_rate_variability:
            print("  💓 Exporting heart rate variability...")
            hrv_file = self._export_hrv_csv(user_data.heart_rate_variability)
            exported_files["csv"].append(hrv_file)

        # Export Active Zone Minutes to CSV
        if user_data.active_zone_minutes:
            print("  🔥 Exporting active zone minutes...")
            azm_file = self._export_azm_csv(user_data.active_zone_minutes)
            exported_files["csv"].append(azm_file)

        # Export activities (placeholder for now)
        if user_data.activities:
            print("  🏃 Exporting activities summary...")
            activity_files = self._export_activities_summary_csv(user_data.activities)
            exported_files["csv"].extend(activity_files)

        # Export heart rate zones from activities
        activities_with_zones = [
            a
            for a in user_data.activities
            if a.heart_rate_zones or a.recalculated_hr_zones
        ]
        if activities_with_zones:
            print("  💓 Exporting heart rate zones...")
            hr_zones_file = self._export_heart_rate_zones_csv(activities_with_zones)
            exported_files["csv"].append(hr_zones_file)

        # Export stress data
        if user_data.stress_data:
            print("  🧘 Exporting stress data...")
            stress_file = self._export_stress_csv(user_data.stress_data)
            exported_files["csv"].append(stress_file)

        # Export skin temperature data
        if user_data.temperature_data:
            print("  🌡️ Exporting skin temperature data...")
            temp_file = self._export_temperature_csv(user_data.temperature_data)
            exported_files["csv"].append(temp_file)

        # Export SpO2 data
        if user_data.spo2_data:
            print("  🩸 Exporting SpO2 data...")
            spo2_file = self._export_spo2_csv(user_data.spo2_data)
            exported_files["csv"].append(spo2_file)

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
                steps_data.append(
                    {"Date": metric.date.strftime("%Y-%m-%d"), "Steps": metric.steps}
                )

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
                distance_data.append(
                    {
                        "Date": metric.date.strftime("%Y-%m-%d"),
                        "Distance (km)": metric.distance,
                    }
                )

        if distance_data:
            df = pd.DataFrame(distance_data)
            df.to_csv(distance_file, index=False)
            csv_files.append(str(distance_file))
            logger.info(
                f"Exported {len(distance_data)} distance records to {distance_file}"
            )

        # Calories data
        calories_file = self.output_dir / "fitbit_calories.csv"
        calories_data = []
        for metric in daily_metrics:
            if metric.calories_burned is not None:
                calories_data.append(
                    {
                        "Date": metric.date.strftime("%Y-%m-%d"),
                        "Calories Burned": metric.calories_burned,
                        "Calories BMR": metric.calories_bmr or 0,
                    }
                )

        if calories_data:
            df = pd.DataFrame(calories_data)
            df.to_csv(calories_file, index=False)
            csv_files.append(str(calories_file))
            logger.info(
                f"Exported {len(calories_data)} calories records to {calories_file}"
            )

        # Activity minutes data
        activity_file = self.output_dir / "fitbit_activity_minutes.csv"
        activity_data = []
        for metric in daily_metrics:
            if any(
                [
                    metric.sedentary_minutes,
                    metric.lightly_active_minutes,
                    metric.fairly_active_minutes,
                    metric.very_active_minutes,
                ]
            ):
                activity_data.append(
                    {
                        "Date": metric.date.strftime("%Y-%m-%d"),
                        "Sedentary Minutes": metric.sedentary_minutes or 0,
                        "Lightly Active Minutes": metric.lightly_active_minutes or 0,
                        "Fairly Active Minutes": metric.fairly_active_minutes or 0,
                        "Very Active Minutes": metric.very_active_minutes or 0,
                        "Active Minutes": metric.active_minutes or 0,
                    }
                )

        if activity_data:
            df = pd.DataFrame(activity_data)
            df.to_csv(activity_file, index=False)
            csv_files.append(str(activity_file))
            logger.info(
                f"Exported {len(activity_data)} activity minutes records to {activity_file}"
            )

        # Floors data
        floors_file = self.output_dir / "fitbit_floors.csv"
        floors_data = []
        for metric in daily_metrics:
            if metric.floors is not None:
                floors_data.append(
                    {"Date": metric.date.strftime("%Y-%m-%d"), "Floors": metric.floors}
                )

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
            sleep_records.append(
                {
                    "Date": sleep.date_of_sleep.strftime("%Y-%m-%d"),
                    "Start Time": sleep.start_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "End Time": sleep.end_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "Duration (hours)": sleep.total_sleep_hours,
                    "Minutes Asleep": sleep.minutes_asleep or 0,
                    "Minutes Awake": sleep.minutes_awake or 0,
                    "Minutes REM": sleep.minutes_rem or 0,
                    "Minutes Light Sleep": sleep.minutes_light or 0,
                    "Minutes Deep Sleep": sleep.minutes_deep or 0,
                    "Minutes Wake in Sleep": sleep.minutes_wake or 0,
                    "Sleep Efficiency": sleep.efficiency or 0,
                    "Sleep Score": sleep.sleep_score or "",
                    "Awakening Count": sleep.awakening_count or "",
                    "Breathing Rate": sleep.breathing_rate or "",
                    "Avg HR During Sleep": sleep.heart_rate_avg or "",
                    "Min HR During Sleep": sleep.heart_rate_min or "",
                    "Skin Temp Deviation": sleep.temperature_variation or "",
                    "Sleep Type": sleep.type or "",
                    "Minutes to Fall Asleep": sleep.minutes_to_fall_asleep or 0,
                    "Minutes After Wakeup": sleep.minutes_after_wakeup or 0,
                    "Time in Bed": sleep.time_in_bed or 0,
                }
            )

        df = pd.DataFrame(sleep_records)
        df.to_csv(sleep_file, index=False)
        logger.info(f"Exported {len(sleep_records)} sleep records to {sleep_file}")

        return str(sleep_file)

    def _export_body_composition_csv(self, body_data: List[BodyComposition]) -> str:
        """Export body composition data to two CSV files:
        1. fitbit_body_composition.csv — full raw data for reference.
        2. garmin_body_composition_import.csv — Garmin Connect importable format.

        Garmin Connect CSV body import format (verified):
          Line 1: "Body"
          Line 2: header — Date,Weight,BMI,Fat
          Data:   weight in kg, fat as percentage (e.g. 18.5 = 18.5%)

        Weight unit heuristic: Fitbit exports weight in lbs for US-locale accounts and
        in kg for metric accounts. No unit field is present in the JSON. Values above
        _LBS_THRESHOLD (100) are assumed to be in lbs and are converted to kg.
        Verify the output if your weight is near this threshold.
        """
        body_file = self.output_dir / "fitbit_body_composition.csv"
        garmin_file = self.output_dir / "garmin_body_composition_import.csv"

        body_records = []
        garmin_records = []

        for body in body_data:
            # Raw data export (label clearly that unit depends on Fitbit account locale)
            body_records.append(
                {
                    "Date": body.date.strftime("%Y-%m-%d"),
                    "Weight (raw, lbs for US / kg for metric)": body.weight,
                    "BMI": body.bmi,
                    "Body Fat %": body.body_fat_percentage,
                    "Lean Mass (raw)": body.lean_mass,
                    "Muscle Mass (raw)": body.muscle_mass,
                    "Bone Mass (raw)": body.bone_mass,
                    "Water %": body.water_percentage,
                }
            )

            # Garmin Connect importable format — weight must be in kg
            if body.weight is not None:
                weight_kg = (
                    round(body.weight * _LBS_TO_KG, 2)
                    if body.weight > _LBS_THRESHOLD
                    else round(body.weight, 2)
                )
                garmin_records.append(
                    {
                        "Date": body.date.strftime("%Y-%m-%d"),
                        "Weight": weight_kg,
                        "BMI": round(body.bmi, 2) if body.bmi else "",
                        "Fat": round(body.body_fat_percentage, 2) if body.body_fat_percentage else "",
                    }
                )

        df = pd.DataFrame(body_records)
        df.to_csv(body_file, index=False)
        logger.info(f"Exported {len(body_records)} body composition records to {body_file}")

        if garmin_records:
            with open(garmin_file, "w", newline="", encoding="utf-8") as f:
                f.write("Body\n")
                writer = csv.DictWriter(f, fieldnames=["Date", "Weight", "BMI", "Fat"])
                writer.writeheader()
                writer.writerows(garmin_records)
            logger.info(f"Created Garmin Connect importable body composition file: {garmin_file}")
            print(f"    ℹ️  garmin_body_composition_import.csv ready — upload at Garmin Connect > Import Data")

        return str(body_file)

    def _export_heart_rate_csv(self, heart_rate_data: List[HeartRateData]) -> str:
        """Export heart rate data to CSV format."""
        hr_file = self.output_dir / "fitbit_heart_rate.csv"

        # Group by date to get daily summaries
        daily_hr = {}
        for hr in heart_rate_data:
            date_key = hr.datetime.date()
            if date_key not in daily_hr:
                daily_hr[date_key] = {"readings": [], "high_confidence": []}

            daily_hr[date_key]["readings"].append(hr.bpm)
            if hr.confidence >= 2:  # High confidence readings
                daily_hr[date_key]["high_confidence"].append(hr.bpm)

        hr_records = []
        for date_key, data in daily_hr.items():
            readings = data["readings"]
            high_conf = data["high_confidence"] if data["high_confidence"] else readings

            hr_records.append(
                {
                    "Date": date_key.strftime("%Y-%m-%d"),
                    "Average Heart Rate": sum(readings) / len(readings)
                    if readings
                    else 0,
                    "Min Heart Rate": min(readings) if readings else 0,
                    "Max Heart Rate": max(readings) if readings else 0,
                    "Resting Heart Rate": min(high_conf) if high_conf else 0,
                    "Total Readings": len(readings),
                    "High Confidence Readings": len(high_conf),
                }
            )

        df = pd.DataFrame(hr_records)
        df.to_csv(hr_file, index=False)
        logger.info(
            f"Exported {len(hr_records)} daily heart rate summaries to {hr_file}"
        )

        return str(hr_file)

    def _export_activities_summary_csv(
        self, activities: List[ActivityData]
    ) -> List[str]:
        """Export activities summary to CSV format."""
        activities_file = self.output_dir / "fitbit_activities.csv"

        activity_records = []
        for activity in activities:
            activity_records.append(
                {
                    "Date": activity.start_time.strftime("%Y-%m-%d"),
                    "Start Time": activity.start_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "Activity Name": activity.activity_name,
                    "Activity Type": activity.activity_type.value,
                    "Duration (minutes)": activity.duration_ms / 60000,
                    "Calories": activity.calories or 0,
                    "Distance (km)": activity.distance or 0,
                    "Steps": activity.steps or 0,
                    "Average Heart Rate": activity.average_heart_rate or 0,
                    "Max Heart Rate": activity.max_heart_rate or 0,
                    "Has GPS Data": bool(activity.gps_data),
                    "Has TCX Data": bool(activity.tcx_data),
                }
            )

        df = pd.DataFrame(activity_records)
        df.to_csv(activities_file, index=False)
        logger.info(f"Exported {len(activity_records)} activities to {activities_file}")

        return [str(activities_file)]

    def _export_hrv_csv(self, hrv_data: List[HeartRateVariability]) -> str:
        """Export heart rate variability data to CSV format."""
        hrv_file = self.output_dir / "fitbit_heart_rate_variability.csv"

        hrv_records = []
        for hrv in hrv_data:
            hrv_records.append(
                {
                    "Date": hrv.date.strftime("%Y-%m-%d"),
                    "RMSSD": hrv.rmssd,
                    "Coverage": hrv.coverage,
                    "Low Frequency": hrv.low_frequency,
                    "High Frequency": hrv.high_frequency,
                    "Timestamp": hrv.timestamp.strftime("%Y-%m-%d %H:%M:%S")
                    if hrv.timestamp
                    else "",
                }
            )

        df = pd.DataFrame(hrv_records)
        df.to_csv(hrv_file, index=False)
        logger.info(f"Exported {len(hrv_records)} HRV records to {hrv_file}")

        return str(hrv_file)

    def _export_azm_csv(self, azm_data: List[ActiveZoneMinutes]) -> str:
        """Export Active Zone Minutes data to CSV format."""
        azm_file = self.output_dir / "fitbit_active_zone_minutes.csv"

        azm_records = []
        for azm in azm_data:
            azm_records.append(
                {
                    "Date": azm.date.strftime("%Y-%m-%d"),
                    "Fat Burn Minutes": azm.fat_burn_minutes,
                    "Cardio Minutes": azm.cardio_minutes,
                    "Peak Minutes": azm.peak_minutes,
                    "Total Minutes": azm.total_minutes,
                }
            )

        df = pd.DataFrame(azm_records)
        df.to_csv(azm_file, index=False)
        logger.info(
            f"Exported {len(azm_records)} Active Zone Minutes records to {azm_file}"
        )

        return str(azm_file)

    def _export_heart_rate_zones_csv(self, activities: List[ActivityData]) -> str:
        """Export heart rate zones from activities to CSV format."""
        hr_zones_file = self.output_dir / "fitbit_heart_rate_zones.csv"

        zone_records = []
        for activity in activities:
            # Use recalculated zones if available, otherwise use original zones
            zones_to_export = (
                activity.recalculated_hr_zones
                if activity.recalculated_hr_zones
                else activity.heart_rate_zones
            )

            if zones_to_export:
                base_record = {
                    "Date": activity.start_time.strftime("%Y-%m-%d"),
                    "Activity": activity.activity_name,
                    "Activity Type": activity.activity_type.value,
                    "Start Time": activity.start_time.strftime("%H:%M:%S"),
                    "Duration (minutes)": activity.duration_ms / 60000,
                    "Average HR": activity.average_heart_rate,
                    "Max HR": activity.max_heart_rate,
                    "Min HR": activity.min_heart_rate,
                    "Calculated Max HR": activity.max_heart_rate_calculated,
                    "Resting HR": activity.resting_heart_rate,
                    "HR Reserve": activity.heart_rate_reserve,
                    "Zone Source": "Recalculated"
                    if activity.recalculated_hr_zones
                    else "Original",
                }

                # Add zone-specific data
                for i, zone in enumerate(zones_to_export, 1):
                    zone_record = base_record.copy()
                    zone_record.update(
                        {
                            "Zone Number": zone.zone_index or i,
                            "Zone Name": zone.name,
                            "Garmin Zone Name": zone.garmin_zone_name or zone.name,
                            "Min BPM": zone.min_bpm,
                            "Max BPM": zone.max_bpm,
                            "Time in Zone (minutes)": zone.minutes,
                            "Calories in Zone": zone.calories_out,
                            "Percentage Max HR": zone.percentage_max_hr,
                            "Percentage HR Reserve": zone.percentage_hr_reserve,
                        }
                    )
                    zone_records.append(zone_record)

        # Sort by date and activity
        zone_records.sort(key=lambda x: (x["Date"], x["Start Time"]))

        if zone_records:
            df = pd.DataFrame(zone_records)
            df.to_csv(hr_zones_file, index=False)
            logger.info(
                f"Exported {len(zone_records)} heart rate zone records to {hr_zones_file}"
            )
        else:
            # Create empty file with headers
            headers = [
                "Date",
                "Activity",
                "Activity Type",
                "Start Time",
                "Duration (minutes)",
                "Average HR",
                "Max HR",
                "Min HR",
                "Calculated Max HR",
                "Resting HR",
                "HR Reserve",
                "Zone Source",
                "Zone Number",
                "Zone Name",
                "Garmin Zone Name",
                "Min BPM",
                "Max BPM",
                "Time in Zone (minutes)",
                "Calories in Zone",
                "Percentage Max HR",
                "Percentage HR Reserve",
            ]
            df = pd.DataFrame(columns=headers)
            df.to_csv(hr_zones_file, index=False)
            logger.info(f"Created empty heart rate zones file: {hr_zones_file}")

        return str(hr_zones_file)

    def _export_stress_csv(self, stress_data: List[StressData]) -> str:
        """Export stress management score data to CSV."""
        stress_file = self.output_dir / "fitbit_stress.csv"

        records = []
        for stress in stress_data:
            records.append(
                {
                    "Date": stress.date.strftime("%Y-%m-%d"),
                    "Stress Score (0-100, higher = less stress)": stress.stress_score or "",
                    "Stress Level": stress.stress_level or "",
                }
            )

        df = pd.DataFrame(records)
        df.to_csv(stress_file, index=False)
        logger.info(f"Exported {len(records)} stress records to {stress_file}")
        return str(stress_file)

    def _export_temperature_csv(self, temperature_data: List[TemperatureData]) -> str:
        """Export skin temperature deviation data to CSV.

        Note: This is a nightly relative deviation from the user's personal baseline,
        NOT an absolute body temperature. Positive = warmer, negative = cooler.
        """
        temp_file = self.output_dir / "fitbit_skin_temperature.csv"

        records = []
        for temp in temperature_data:
            records.append(
                {
                    "Date": temp.date.strftime("%Y-%m-%d"),
                    "Skin Temp Deviation (°C from baseline)": temp.temperature_celsius or "",
                }
            )

        df = pd.DataFrame(records)
        df.to_csv(temp_file, index=False)
        logger.info(f"Exported {len(records)} skin temperature records to {temp_file}")
        return str(temp_file)

    def _export_spo2_csv(self, spo2_data: List[SpO2Data]) -> str:
        """Export blood oxygen saturation (SpO2) data to CSV."""
        spo2_file = self.output_dir / "fitbit_spo2.csv"

        records = []
        for spo2 in spo2_data:
            records.append(
                {
                    "Date": spo2.date.strftime("%Y-%m-%d"),
                    "SpO2 %": spo2.spo2_percentage or "",
                    "Timestamp": spo2.timestamp.strftime("%Y-%m-%d %H:%M:%S")
                    if spo2.timestamp
                    else "",
                }
            )

        df = pd.DataFrame(records)
        df.to_csv(spo2_file, index=False)
        logger.info(f"Exported {len(records)} SpO2 records to {spo2_file}")
        return str(spo2_file)

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

            daily_data_map[date_key].update(
                {
                    "Steps": metric.steps,
                    "Distance": metric.distance,
                    "Calories": metric.calories_burned,
                    "Floors": metric.floors,
                    "Active Minutes": metric.active_minutes,
                }
            )

        # Add sleep data
        for sleep in user_data.sleep_data:
            date_key = sleep.date_of_sleep
            if date_key not in daily_data_map:
                daily_data_map[date_key] = {}

            daily_data_map[date_key].update(
                {
                    "Sleep Duration (hours)": sleep.total_sleep_hours,
                    "Sleep Efficiency": sleep.efficiency,
                }
            )

        # Add body composition data
        for body in user_data.body_composition:
            date_key = body.date
            if date_key not in daily_data_map:
                daily_data_map[date_key] = {}

            daily_data_map[date_key].update(
                {
                    "Weight": body.weight,
                    "BMI": body.bmi,
                    "Body Fat %": body.body_fat_percentage,
                }
            )

        # Add HRV data
        for hrv in user_data.heart_rate_variability:
            date_key = hrv.date
            if date_key not in daily_data_map:
                daily_data_map[date_key] = {}

            daily_data_map[date_key].update({"HRV RMSSD": hrv.rmssd})

        # Add Active Zone Minutes data
        for azm in user_data.active_zone_minutes:
            date_key = azm.date
            if date_key not in daily_data_map:
                daily_data_map[date_key] = {}

            daily_data_map[date_key].update(
                {
                    "Fat Burn Minutes": azm.fat_burn_minutes,
                    "Cardio Minutes": azm.cardio_minutes,
                    "Peak Minutes": azm.peak_minutes,
                    "Total AZM": azm.total_minutes,
                }
            )

        # Convert to list format
        for date_key, data in sorted(daily_data_map.items()):
            record = {"Date": date_key.strftime("%Y-%m-%d")}
            record.update(data)
            import_data.append(record)

        # Export to CSV
        if import_data:
            df = pd.DataFrame(import_data)
            df.to_csv(import_file, index=False)
            logger.info(f"Created Garmin Connect import file: {import_file}")

        return {
            "garmin_import": str(import_file),
            "date_range": f"{user_data.date_range[0]} to {user_data.date_range[1]}",
            "total_records": len(import_data),
        }
