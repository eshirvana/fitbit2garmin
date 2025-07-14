"""
Data models for Fitbit data types using Pydantic for validation and serialization.
"""

from datetime import datetime, date
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field, validator
from enum import Enum


class ActivityType(str, Enum):
    """Supported activity types."""
    RUN = "run"
    WALK = "walk"
    BIKE = "bike"
    HIKE = "hike"
    SWIM = "swim"
    WORKOUT = "workout"
    YOGA = "yoga"
    WEIGHTS = "weights"
    SPORT = "sport"
    OTHER = "other"


class SleepStage(str, Enum):
    """Sleep stage types."""
    AWAKE = "awake"
    LIGHT = "light"
    DEEP = "deep"
    REM = "rem"
    RESTLESS = "restless"
    ASLEEP = "asleep"


class HeartRateZone(BaseModel):
    """Heart rate zone data."""
    name: str
    min_bpm: int
    max_bpm: int
    minutes: int
    calories_out: Optional[float] = None


class HeartRateData(BaseModel):
    """Heart rate measurement with timestamp."""
    datetime: datetime
    bpm: int
    confidence: int = Field(ge=0, le=3, description="Confidence level 0-3")


class ActivityData(BaseModel):
    """Activity/exercise data from Fitbit."""
    log_id: int
    activity_name: str
    activity_type: ActivityType
    start_time: datetime
    duration_ms: int
    calories: Optional[int] = None
    distance: Optional[float] = None
    steps: Optional[int] = None
    heart_rate_zones: List[HeartRateZone] = []
    average_heart_rate: Optional[int] = None
    max_heart_rate: Optional[int] = None
    gps_data: Optional[Union[List[Dict[str, Any]], str]] = None
    tcx_data: Optional[str] = None
    
    @validator('activity_type', pre=True)
    def parse_activity_type(cls, v):
        """Parse activity type from Fitbit format."""
        if isinstance(v, str):
            v = v.lower()
            # Map common Fitbit activity names to our enum
            type_mapping = {
                'running': ActivityType.RUN,
                'walking': ActivityType.WALK,
                'cycling': ActivityType.BIKE,
                'hiking': ActivityType.HIKE,
                'swimming': ActivityType.SWIM,
                'workout': ActivityType.WORKOUT,
                'yoga': ActivityType.YOGA,
                'weights': ActivityType.WEIGHTS,
                'sport': ActivityType.SPORT
            }
            return type_mapping.get(v, ActivityType.OTHER)
        return v


class SleepData(BaseModel):
    """Sleep data from Fitbit."""
    log_id: int
    date_of_sleep: date
    start_time: datetime
    end_time: datetime
    duration_ms: int
    efficiency: Optional[int] = None
    minutes_awake: Optional[int] = None
    minutes_asleep: Optional[int] = None
    minutes_to_fall_asleep: Optional[int] = None
    minutes_after_wakeup: Optional[int] = None
    time_in_bed: Optional[int] = None
    sleep_stages: List[Dict[str, Any]] = []
    
    @property
    def total_sleep_hours(self) -> float:
        """Calculate total sleep time in hours."""
        return (self.minutes_asleep or 0) / 60.0


class DailyMetrics(BaseModel):
    """Daily aggregated metrics from Fitbit."""
    date: date
    steps: Optional[int] = None
    distance: Optional[float] = None
    calories_burned: Optional[int] = None
    calories_bmr: Optional[int] = None
    active_minutes: Optional[int] = None
    sedentary_minutes: Optional[int] = None
    lightly_active_minutes: Optional[int] = None
    fairly_active_minutes: Optional[int] = None
    very_active_minutes: Optional[int] = None
    floors: Optional[int] = None
    elevation: Optional[float] = None
    resting_heart_rate: Optional[int] = None
    heart_rate_zones: List[HeartRateZone] = []


class BodyComposition(BaseModel):
    """Body composition data from Fitbit."""
    date: date
    weight: Optional[float] = None
    bmi: Optional[float] = None
    body_fat_percentage: Optional[float] = None
    lean_mass: Optional[float] = None
    muscle_mass: Optional[float] = None
    bone_mass: Optional[float] = None
    water_percentage: Optional[float] = None


class HeartRateVariability(BaseModel):
    """Heart rate variability data."""
    date: date
    rmssd: Optional[float] = None
    coverage: Optional[float] = None
    low_frequency: Optional[float] = None
    high_frequency: Optional[float] = None
    timestamp: Optional[datetime] = None


class StressData(BaseModel):
    """Stress score data from Fitbit."""
    date: date
    stress_score: Optional[int] = None
    stress_level: Optional[str] = None
    responsiveness_level: Optional[str] = None


class TemperatureData(BaseModel):
    """Temperature data from Fitbit."""
    date: date
    temperature_celsius: Optional[float] = None
    temperature_fahrenheit: Optional[float] = None


class SpO2Data(BaseModel):
    """Blood oxygen saturation data."""
    date: date
    spo2_percentage: Optional[float] = None
    timestamp: Optional[datetime] = None


class ActiveZoneMinutes(BaseModel):
    """Active Zone Minutes data."""
    date: date
    fat_burn_minutes: Optional[int] = None
    cardio_minutes: Optional[int] = None
    peak_minutes: Optional[int] = None
    total_minutes: Optional[int] = None


class FitbitUserData(BaseModel):
    """Complete user data from Fitbit export."""
    activities: List[ActivityData] = []
    sleep_data: List[SleepData] = []
    daily_metrics: List[DailyMetrics] = []
    body_composition: List[BodyComposition] = []
    heart_rate_data: List[HeartRateData] = []
    heart_rate_variability: List[HeartRateVariability] = []
    stress_data: List[StressData] = []
    temperature_data: List[TemperatureData] = []
    spo2_data: List[SpO2Data] = []
    active_zone_minutes: List[ActiveZoneMinutes] = []
    
    @property
    def date_range(self) -> tuple[date, date]:
        """Get the date range of all data."""
        dates = []
        
        # Collect all dates from different data types
        for activity in self.activities:
            dates.append(activity.start_time.date())
        for sleep in self.sleep_data:
            dates.append(sleep.date_of_sleep)
        for daily in self.daily_metrics:
            dates.append(daily.date)
        for body in self.body_composition:
            dates.append(body.date)
        for hr in self.heart_rate_data:
            dates.append(hr.datetime.date())
        
        if not dates:
            return date.today(), date.today()
        
        return min(dates), max(dates)
    
    @property
    def total_activities(self) -> int:
        """Get total number of activities."""
        return len(self.activities)
    
    @property
    def total_sleep_records(self) -> int:
        """Get total number of sleep records."""
        return len(self.sleep_data)
    
    @property
    def total_daily_records(self) -> int:
        """Get total number of daily metric records."""
        return len(self.daily_metrics)