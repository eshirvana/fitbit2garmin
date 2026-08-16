"""Activity type -> (Sport, SubSport) mapping.

Both tables are derived directly from this user's real data (16 activity_name
values from UserExercises_*.csv, 15 activityTypeId values from exercise-*.json) --
not a generic guessed-at ID table. Confirmed against the real installed fit-tool
enum (fit_tool.profile.profile_type): YOGA, PILATES, STRENGTH_TRAINING all exist,
no library swap needed.

Precedence when both are available: activity_type_id (more granular Fitbit
taxonomy) over activity_name.
"""

from dataclasses import dataclass

from fit_tool.profile.profile_type import Sport, SubSport


@dataclass(frozen=True)
class SportMapping:
    sport: Sport
    sub_sport: SubSport


# ASSUMPTION (flagged in plan, verify in the Phase 1 validation batch):
# - CrossFit -> TRAINING/CARDIO_TRAINING: FIT has no literal CrossFit type.
# - Outdoor Walk -> WALKING/CASUAL_WALKING chosen over STREET as more idiomatic.
ACTIVITY_NAME_MAP: dict[str, SportMapping] = {
    "10 Minute Abs": SportMapping(Sport.TRAINING, SubSport.STRENGTH_TRAINING),
    "Aerobic Workout": SportMapping(Sport.TRAINING, SubSport.CARDIO_TRAINING),
    "Bike": SportMapping(Sport.CYCLING, SubSport.GENERIC),
    "CrossFit": SportMapping(Sport.TRAINING, SubSport.CARDIO_TRAINING),
    "Elliptical": SportMapping(Sport.FITNESS_EQUIPMENT, SubSport.ELLIPTICAL),
    "Hike": SportMapping(Sport.HIKING, SubSport.GENERIC),
    "Outdoor Bike": SportMapping(Sport.CYCLING, SubSport.ROAD),
    "Outdoor Run": SportMapping(Sport.RUNNING, SubSport.STREET),
    "Outdoor Walk": SportMapping(Sport.WALKING, SubSport.CASUAL_WALKING),
    "Sport": SportMapping(Sport.GENERIC, SubSport.GENERIC),
    "Swim": SportMapping(Sport.SWIMMING, SubSport.LAP_SWIMMING),
    "Tennis": SportMapping(Sport.TENNIS, SubSport.GENERIC),
    "Treadmill Run": SportMapping(Sport.RUNNING, SubSport.TREADMILL),
    "Warm It Up": SportMapping(Sport.TRAINING, SubSport.WARM_UP),
    "Weights": SportMapping(Sport.TRAINING, SubSport.STRENGTH_TRAINING),
    "Workout": SportMapping(Sport.TRAINING, SubSport.EXERCISE),
}

ACTIVITY_TYPE_ID_MAP: dict[int, SportMapping] = {
    1071: SportMapping(Sport.CYCLING, SubSport.ROAD),          # Outdoor Bike
    15000: SportMapping(Sport.GENERIC, SubSport.GENERIC),      # Sport
    15675: SportMapping(Sport.TENNIS, SubSport.GENERIC),       # Tennis
    20047: SportMapping(Sport.FITNESS_EQUIPMENT, SubSport.ELLIPTICAL),  # Elliptical
    20049: SportMapping(Sport.RUNNING, SubSport.TREADMILL),    # Treadmill
    2131: SportMapping(Sport.TRAINING, SubSport.STRENGTH_TRAINING),   # Weights
    3000: SportMapping(Sport.TRAINING, SubSport.EXERCISE),     # Workout
    3001: SportMapping(Sport.TRAINING, SubSport.CARDIO_TRAINING),  # Aerobic Workout
    3101: SportMapping(Sport.TRAINING, SubSport.STRENGTH_TRAINING),   # 10 Minute Abs
    3102: SportMapping(Sport.TRAINING, SubSport.WARM_UP),      # Warm It Up
    90001: SportMapping(Sport.CYCLING, SubSport.GENERIC),      # Bike
    90009: SportMapping(Sport.RUNNING, SubSport.STREET),       # Run
    90012: SportMapping(Sport.HIKING, SubSport.GENERIC),       # Hike
    90013: SportMapping(Sport.WALKING, SubSport.CASUAL_WALKING),  # Walk
    91045: SportMapping(Sport.TRAINING, SubSport.CARDIO_TRAINING),  # CrossFit
}

# GPS-plausible names/ids: candidates for the gps_location_csv time-window fallback
# in reconcile/gps_attacher.py when no TCX file matches.
GPS_PLAUSIBLE_NAMES = {
    "Bike", "Outdoor Bike", "Outdoor Run", "Outdoor Walk", "Hike",
}
GPS_PLAUSIBLE_TYPE_IDS = {1071, 90001, 90009, 90012, 90013}


def resolve_sport(
    activity_name: str, activity_type_id: int | None
) -> tuple[SportMapping, bool]:
    """Returns (mapping, was_unmapped). activity_type_id takes precedence when
    present and known; falls back to activity_name; falls back to GENERIC/GENERIC
    (flagged unmapped) if neither table has an entry -- never raises."""
    if activity_type_id is not None and activity_type_id in ACTIVITY_TYPE_ID_MAP:
        return ACTIVITY_TYPE_ID_MAP[activity_type_id], False
    if activity_name in ACTIVITY_NAME_MAP:
        return ACTIVITY_NAME_MAP[activity_name], False
    return SportMapping(Sport.GENERIC, SubSport.GENERIC), True


def apply_gps_refinement(mapping: SportMapping, gps_attached: bool) -> SportMapping:
    """Upgrade a GENERIC sub-sport to a GPS-specific one when a real GPS track is
    attached -- applied after the table lookup so both tables stay purely
    declarative and independently testable."""
    if not gps_attached:
        return mapping
    if mapping.sport == Sport.CYCLING and mapping.sub_sport == SubSport.GENERIC:
        return SportMapping(Sport.CYCLING, SubSport.ROAD)
    if mapping.sport == Sport.RUNNING and mapping.sub_sport == SubSport.GENERIC:
        return SportMapping(Sport.RUNNING, SubSport.STREET)
    if mapping.sport == Sport.SWIMMING and mapping.sub_sport == SubSport.LAP_SWIMMING:
        return SportMapping(Sport.SWIMMING, SubSport.OPEN_WATER)
    return mapping
