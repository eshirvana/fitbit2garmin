"""
Heart rate zone calculations and mapping utilities for Fitbit to Garmin conversion.
Provides age-based zone calculations, heart rate reserve calculations, and Garmin compatibility.
"""

import logging
from datetime import datetime, date
from typing import List, Optional, Dict, Any, Tuple
from dataclasses import dataclass

from .models import HeartRateZone, ActivityData, DailyMetrics

logger = logging.getLogger(__name__)


@dataclass
class UserProfile:
    """User profile data for heart rate zone calculations."""

    age: Optional[int] = None
    resting_heart_rate: Optional[int] = None
    max_heart_rate: Optional[int] = None
    gender: Optional[str] = None  # 'male' or 'female'
    fitness_level: Optional[str] = None  # 'beginner', 'intermediate', 'advanced'


class HeartRateZoneCalculator:
    """Calculator for heart rate zones with multiple methods and Garmin compatibility."""

    # Standard heart rate zone definitions (percentage of max HR)
    ZONE_DEFINITIONS = {
        "5_zone_system": {
            1: {
                "name": "Recovery",
                "min_pct": 50,
                "max_pct": 60,
                "garmin_name": "Zone 1",
            },
            2: {
                "name": "Aerobic",
                "min_pct": 60,
                "max_pct": 70,
                "garmin_name": "Zone 2",
            },
            3: {"name": "Tempo", "min_pct": 70, "max_pct": 80, "garmin_name": "Zone 3"},
            4: {
                "name": "Threshold",
                "min_pct": 80,
                "max_pct": 90,
                "garmin_name": "Zone 4",
            },
            5: {
                "name": "Anaerobic",
                "min_pct": 90,
                "max_pct": 100,
                "garmin_name": "Zone 5",
            },
        },
        "garmin_standard": {
            1: {
                "name": "Active Recovery",
                "min_pct": 50,
                "max_pct": 60,
                "garmin_name": "Zone 1",
            },
            2: {
                "name": "Aerobic Base",
                "min_pct": 60,
                "max_pct": 70,
                "garmin_name": "Zone 2",
            },
            3: {
                "name": "Aerobic",
                "min_pct": 70,
                "max_pct": 80,
                "garmin_name": "Zone 3",
            },
            4: {
                "name": "Lactate Threshold",
                "min_pct": 80,
                "max_pct": 90,
                "garmin_name": "Zone 4",
            },
            5: {
                "name": "Neuromuscular",
                "min_pct": 90,
                "max_pct": 100,
                "garmin_name": "Zone 5",
            },
        },
        "fitbit_standard": {
            1: {
                "name": "Fat Burn",
                "min_pct": 50,
                "max_pct": 69,
                "garmin_name": "Zone 1-2",
            },
            2: {
                "name": "Cardio",
                "min_pct": 70,
                "max_pct": 84,
                "garmin_name": "Zone 3-4",
            },
            3: {"name": "Peak", "min_pct": 85, "max_pct": 100, "garmin_name": "Zone 5"},
        },
    }

    def __init__(self, user_profile: Optional[UserProfile] = None):
        """Initialize calculator with optional user profile."""
        self.user_profile = user_profile or UserProfile()

    def estimate_max_heart_rate(self, age: int, method: str = "tanaka") -> int:
        """Estimate maximum heart rate using various formulas."""
        if method == "tanaka":
            # Tanaka formula: 208 - (0.7 × age) - more accurate for adults
            return int(208 - (0.7 * age))
        elif method == "fox":
            # Fox formula: 220 - age - traditional formula
            return int(220 - age)
        elif method == "gellish":
            # Gellish formula: 207 - (0.7 × age) - based on larger study
            return int(207 - (0.7 * age))
        elif method == "nes":
            # Nes formula: 211 - (0.64 × age) - for active individuals
            return int(211 - (0.64 * age))
        else:
            # Default to Tanaka (most accurate)
            return int(208 - (0.7 * age))

    def calculate_heart_rate_reserve(self, max_hr: int, resting_hr: int) -> int:
        """Calculate heart rate reserve (max HR - resting HR)."""
        return max_hr - resting_hr

    def get_effective_max_heart_rate(self) -> Optional[int]:
        """Get the most appropriate max heart rate for the user."""
        # Priority: measured max HR > calculated from age > None
        if self.user_profile.max_heart_rate:
            return self.user_profile.max_heart_rate
        elif self.user_profile.age:
            return self.estimate_max_heart_rate(self.user_profile.age)
        return None

    def get_effective_resting_heart_rate(self) -> Optional[int]:
        """Get the most appropriate resting heart rate for the user."""
        # Use provided resting HR or estimate based on fitness level
        if self.user_profile.resting_heart_rate:
            return self.user_profile.resting_heart_rate
        elif self.user_profile.fitness_level:
            # Rough estimates based on fitness level
            estimates = {"beginner": 70, "intermediate": 65, "advanced": 55}
            return estimates.get(self.user_profile.fitness_level, 65)
        return None

    def calculate_zone_boundaries_percentage(
        self, max_hr: int, zone_system: str = "garmin_standard"
    ) -> List[HeartRateZone]:
        """Calculate heart rate zones based on percentage of max HR."""
        zones = []
        zone_defs = self.ZONE_DEFINITIONS.get(
            zone_system, self.ZONE_DEFINITIONS["garmin_standard"]
        )

        for zone_idx, zone_def in zone_defs.items():
            min_hr = int((zone_def["min_pct"] / 100) * max_hr)
            max_hr_zone = int((zone_def["max_pct"] / 100) * max_hr)

            zone = HeartRateZone(
                name=zone_def["name"],
                min_bpm=min_hr,
                max_bpm=max_hr_zone,
                minutes=0,  # Will be calculated based on activity data
                zone_index=zone_idx,
                percentage_max_hr=(zone_def["min_pct"] + zone_def["max_pct"]) / 2,
                garmin_zone_name=zone_def["garmin_name"],
            )
            zones.append(zone)

        return zones

    def calculate_zone_boundaries_karvonen(
        self, max_hr: int, resting_hr: int, zone_system: str = "garmin_standard"
    ) -> List[HeartRateZone]:
        """Calculate heart rate zones using Karvonen formula (heart rate reserve method)."""
        zones = []
        zone_defs = self.ZONE_DEFINITIONS.get(
            zone_system, self.ZONE_DEFINITIONS["garmin_standard"]
        )
        hr_reserve = self.calculate_heart_rate_reserve(max_hr, resting_hr)

        for zone_idx, zone_def in zone_defs.items():
            # Karvonen formula: Target HR = ((Max HR - Resting HR) × %Intensity) + Resting HR
            min_hr = int(((hr_reserve * zone_def["min_pct"]) / 100) + resting_hr)
            max_hr_zone = int(((hr_reserve * zone_def["max_pct"]) / 100) + resting_hr)

            zone = HeartRateZone(
                name=zone_def["name"],
                min_bpm=min_hr,
                max_bpm=max_hr_zone,
                minutes=0,  # Will be calculated based on activity data
                zone_index=zone_idx,
                percentage_max_hr=(zone_def["min_pct"] + zone_def["max_pct"]) / 2,
                percentage_hr_reserve=(zone_def["min_pct"] + zone_def["max_pct"]) / 2,
                garmin_zone_name=zone_def["garmin_name"],
            )
            zones.append(zone)

        return zones

    def map_fitbit_zones_to_garmin(
        self, fitbit_zones: List[HeartRateZone]
    ) -> List[HeartRateZone]:
        """Map Fitbit heart rate zones to Garmin-compatible zones."""
        garmin_zones = []

        # Fitbit typically uses 3 zones, Garmin uses 5
        # We need to intelligently map and expand

        fitbit_zone_mapping = {
            "Fat Burn": {"garmin_zones": [1, 2], "split_ratio": [0.6, 0.4]},
            "Cardio": {"garmin_zones": [3, 4], "split_ratio": [0.5, 0.5]},
            "Peak": {"garmin_zones": [5], "split_ratio": [1.0]},
            "Out of Range": {"garmin_zones": [], "split_ratio": []},
        }

        # Create 5 Garmin zones
        for i in range(1, 6):
            zone = HeartRateZone(
                name=f"Zone {i}",
                min_bpm=0,
                max_bpm=0,
                minutes=0,
                zone_index=i,
                garmin_zone_name=f"Zone {i}",
            )
            garmin_zones.append(zone)

        # Map Fitbit zone minutes to Garmin zones
        for fitbit_zone in fitbit_zones:
            zone_name = fitbit_zone.name
            if zone_name in fitbit_zone_mapping:
                mapping = fitbit_zone_mapping[zone_name]
                for idx, garmin_zone_idx in enumerate(mapping["garmin_zones"]):
                    if garmin_zone_idx <= len(garmin_zones):
                        # Split minutes according to ratio
                        split_minutes = int(
                            fitbit_zone.minutes * mapping["split_ratio"][idx]
                        )
                        garmin_zones[garmin_zone_idx - 1].minutes += split_minutes

                        # Copy HR boundaries if available
                        if fitbit_zone.min_bpm > 0:
                            garmin_zones[
                                garmin_zone_idx - 1
                            ].min_bpm = fitbit_zone.min_bpm
                        if fitbit_zone.max_bpm > 0:
                            garmin_zones[
                                garmin_zone_idx - 1
                            ].max_bpm = fitbit_zone.max_bpm

        return garmin_zones

    def recalculate_activity_zones(self, activity: ActivityData) -> ActivityData:
        """Recalculate heart rate zones for an activity based on user profile."""
        # Get effective max and resting heart rates
        max_hr = self.get_effective_max_heart_rate()
        resting_hr = self.get_effective_resting_heart_rate()

        # Update activity with calculated values
        activity.max_heart_rate_calculated = max_hr
        activity.resting_heart_rate = resting_hr
        if max_hr and resting_hr:
            activity.heart_rate_reserve = max_hr - resting_hr

        # If we have both max and resting HR, use Karvonen method
        if max_hr and resting_hr:
            recalculated_zones = self.calculate_zone_boundaries_karvonen(
                max_hr, resting_hr
            )
        elif max_hr:
            # Use percentage method if only max HR available
            recalculated_zones = self.calculate_zone_boundaries_percentage(max_hr)
        else:
            # Map existing Fitbit zones to Garmin format
            if activity.heart_rate_zones:
                recalculated_zones = self.map_fitbit_zones_to_garmin(
                    activity.heart_rate_zones
                )
            else:
                recalculated_zones = []

        # Redistribute time based on original zones if available
        if activity.heart_rate_zones and recalculated_zones:
            recalculated_zones = self._redistribute_zone_time(
                activity.heart_rate_zones, recalculated_zones
            )

        activity.recalculated_hr_zones = recalculated_zones

        return activity

    def _redistribute_zone_time(
        self, original_zones: List[HeartRateZone], new_zones: List[HeartRateZone]
    ) -> List[HeartRateZone]:
        """Redistribute time spent in zones based on heart rate boundaries."""
        if not original_zones or not new_zones:
            return new_zones

        # Simple redistribution - can be enhanced with more sophisticated logic
        total_minutes = sum(zone.minutes for zone in original_zones)

        # If we have fewer original zones than new zones, distribute proportionally
        if len(original_zones) < len(new_zones):
            # Distribute based on zone position
            for i, new_zone in enumerate(new_zones):
                if i < len(original_zones):
                    new_zone.minutes = original_zones[i].minutes
                else:
                    # Distribute remaining time to higher zones
                    remaining_idx = i - len(original_zones)
                    if remaining_idx < len(original_zones):
                        new_zone.minutes = original_zones[remaining_idx].minutes // 2
        else:
            # More or equal original zones - map directly
            for i, new_zone in enumerate(new_zones):
                if i < len(original_zones):
                    new_zone.minutes = original_zones[i].minutes

        return new_zones

    def estimate_user_profile_from_data(
        self, activities: List[ActivityData], daily_metrics: List[DailyMetrics]
    ) -> UserProfile:
        """Estimate user profile from activity and daily data."""
        profile = UserProfile()

        # Extract resting heart rate from daily metrics
        resting_hrs = [
            dm.resting_heart_rate for dm in daily_metrics if dm.resting_heart_rate
        ]
        if resting_hrs:
            profile.resting_heart_rate = int(sum(resting_hrs) / len(resting_hrs))

        # Extract max heart rate from activities
        max_hrs = [
            activity.max_heart_rate
            for activity in activities
            if activity.max_heart_rate
        ]
        if max_hrs:
            profile.max_heart_rate = max(max_hrs)

        # Estimate fitness level from activity frequency and intensity
        if activities:
            # Simple heuristic based on activity frequency
            activity_days = len(
                set(activity.start_time.date() for activity in activities)
            )
            total_days = (
                max(activity.start_time.date() for activity in activities)
                - min(activity.start_time.date() for activity in activities)
            ).days + 1

            activity_frequency = activity_days / total_days if total_days > 0 else 0

            if activity_frequency > 0.5:  # Active more than 50% of days
                profile.fitness_level = "advanced"
            elif activity_frequency > 0.2:  # Active more than 20% of days
                profile.fitness_level = "intermediate"
            else:
                profile.fitness_level = "beginner"

        return profile

    def validate_heart_rate_zones(self, zones: List[HeartRateZone]) -> List[str]:
        """Validate heart rate zones and return list of issues."""
        issues = []

        if not zones:
            issues.append("No heart rate zones provided")
            return issues

        # Check for overlapping zones
        for i in range(len(zones) - 1):
            if zones[i].max_bpm >= zones[i + 1].min_bpm:
                issues.append(f"Zone {i + 1} overlaps with Zone {i + 2}")

        # Check for gaps between zones
        for i in range(len(zones) - 1):
            if zones[i].max_bpm + 1 < zones[i + 1].min_bpm:
                issues.append(f"Gap between Zone {i + 1} and Zone {i + 2}")

        # Check for reasonable heart rate ranges
        for i, zone in enumerate(zones):
            if zone.min_bpm < 40 or zone.min_bpm > 220:
                issues.append(
                    f"Zone {i + 1} has unrealistic minimum heart rate: {zone.min_bpm}"
                )
            if zone.max_bpm < 40 or zone.max_bpm > 220:
                issues.append(
                    f"Zone {i + 1} has unrealistic maximum heart rate: {zone.max_bpm}"
                )
            if zone.min_bpm >= zone.max_bpm:
                issues.append(
                    f"Zone {i + 1} has invalid range: {zone.min_bpm}-{zone.max_bpm}"
                )

        return issues
