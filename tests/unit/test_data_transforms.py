import pytest
from datetime import timedelta

from elt.dwh.data_transformations import transform_duration


def test_transform_duration_short():
    row = {"Duration": "PT33S"}
    out = transform_duration(row)

    assert out["Duration"] == timedelta(seconds=33)
    assert out["Video_Type"] == "Shorts"


def test_transform_duration_normal():
    row = {"Duration": "PT15M33S"}
    out = transform_duration(row)

    assert out["Duration"] == timedelta(minutes=15, seconds=33)
    assert out["Video_Type"] == "Normal"


def test_transform_duration_boundary_60s_is_shorts():
    row = {"Duration": "PT60S"}
    out = transform_duration(row)

    assert out["Duration"] == timedelta(seconds=60)
    assert out["Video_Type"] == "Shorts"


def test_transform_duration_missing_duration_returns_row_unchanged():
    row = {"Video_ID": "abc123"}  # no Duration key
    out = transform_duration(row)

    assert out == {"Video_ID": "abc123"}
    assert "Video_Type" not in out


def test_transform_duration_invalid_iso_raises():
    row = {"Duration": "NOT_A_DURATION"}
    with pytest.raises(Exception):
        transform_duration(row)


def test_transform_duration_mutates_in_place_and_returns_same_object():
    row = {"Duration": "PT33S"}
    out = transform_duration(row)

    assert out is row  # same dict object (in-place)
    assert row["Duration"] == timedelta(seconds=33)
    assert row["Video_Type"] == "Shorts"