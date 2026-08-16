"""Tests for reconcile/activity_type_map.py -- parametrized over every real
activity_name/activityTypeId value found in the user's actual Takeout export
(see project plan), plus the GPS-refinement rule in isolation."""

import pytest
from fit_tool.profile.profile_type import Sport, SubSport

from fitbit2garmin.reconcile.activity_type_map import (
    ACTIVITY_NAME_MAP,
    ACTIVITY_TYPE_ID_MAP,
    apply_gps_refinement,
    resolve_sport,
)


@pytest.mark.parametrize("name", list(ACTIVITY_NAME_MAP.keys()))
def test_all_real_activity_names_map_without_fallback(name):
    mapping, was_unmapped = resolve_sport(name, activity_type_id=None)
    assert not was_unmapped
    assert mapping == ACTIVITY_NAME_MAP[name]


@pytest.mark.parametrize("type_id", list(ACTIVITY_TYPE_ID_MAP.keys()))
def test_all_real_activity_type_ids_map_without_fallback(type_id):
    mapping, was_unmapped = resolve_sport("irrelevant name", activity_type_id=type_id)
    assert not was_unmapped
    assert mapping == ACTIVITY_TYPE_ID_MAP[type_id]


def test_type_id_takes_precedence_over_name():
    # 90013 is Walk; if the name says something else, the id should still win.
    mapping, was_unmapped = resolve_sport("Some Other Name", activity_type_id=90013)
    assert not was_unmapped
    assert mapping == ACTIVITY_TYPE_ID_MAP[90013]


def test_unknown_name_and_id_falls_back_to_generic_and_is_flagged():
    mapping, was_unmapped = resolve_sport("Completely Unknown Activity", activity_type_id=999999)
    assert was_unmapped
    assert mapping.sport == Sport.GENERIC
    assert mapping.sub_sport == SubSport.GENERIC


def test_unknown_id_falls_back_to_name_table():
    mapping, was_unmapped = resolve_sport("Tennis", activity_type_id=999999)
    assert not was_unmapped
    assert mapping == ACTIVITY_NAME_MAP["Tennis"]


@pytest.mark.parametrize(
    "sport,sub_sport,gps_attached,expected",
    [
        (Sport.CYCLING, SubSport.GENERIC, True, SubSport.ROAD),
        (Sport.CYCLING, SubSport.GENERIC, False, SubSport.GENERIC),
        (Sport.RUNNING, SubSport.GENERIC, True, SubSport.STREET),
        (Sport.RUNNING, SubSport.GENERIC, False, SubSport.GENERIC),
        (Sport.SWIMMING, SubSport.LAP_SWIMMING, True, SubSport.OPEN_WATER),
        (Sport.SWIMMING, SubSport.LAP_SWIMMING, False, SubSport.LAP_SWIMMING),
        # Already-specific sub-sports are left alone regardless of GPS.
        (Sport.CYCLING, SubSport.ROAD, True, SubSport.ROAD),
        (Sport.WALKING, SubSport.CASUAL_WALKING, True, SubSport.CASUAL_WALKING),
    ],
)
def test_gps_refinement_rule(sport, sub_sport, gps_attached, expected):
    from fitbit2garmin.reconcile.activity_type_map import SportMapping

    result = apply_gps_refinement(SportMapping(sport, sub_sport), gps_attached)
    assert result.sub_sport == expected
