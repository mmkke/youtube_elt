import pytest
from unittest.mock import Mock
from psycopg2 import Error
from datetime import timedelta

from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_transformations import transform_duration

@pytest.fixture
def cur():
    return Mock()


RAW_ROW_01 = {
        "video_id": "abc123",
        "title": "My Title",
        "publishedAt": "2026-01-01T00:00:00Z",
        "duration": "PT15M33S",
        "viewCount": 10,
        "likeCount": 2,
        "commentCount": 1,
    }

STAGING_PARAMS_01 = {
        "video_id": "abc123",
        "video_title": "My Title",
        "upload_date": "2026-01-01T00:00:00Z",
        "duration": "PT15M33S",
        "video_views": 10,
        "likes_count": 2,
        "comments_count": 1,
    }

STAGING_ROW_01 = {
        "Video_ID": "abc123",
        "Video_Title": "My Title",
        "Upload_Date": "2026-01-01T00:00:00Z",
        "Duration": "PT15M33S",
        "Video_Views": 10,
        "Likes_Count": 2,
        "Comments_Count": 1,
    }

CORE_PARAMS_01 = {
        "video_id": "abc123",
        "video_title": "My Title",
        "upload_date": "2026-01-01T00:00:00Z",
        "duration": timedelta(seconds=933),
        "video_type": "Normal",
        "video_views": 10,
        "likes_count": 2,
        "comments_count": 1,
    }

RAW_ROW_02 = {
        "video_id": "abc123",
        "title": "New Title",
        "publishedAt": "2026-01-01T00:00:00Z",
        "duration": "PT15M33S",
        "viewCount": 99,
        "likeCount": 7,
        "commentCount": 3,
    }

STAGING_PARAMS_02 = {
        "video_id": "abc123",
        "upload_date": "2026-01-01T00:00:00Z",
        "video_title": "New Title",
        "duration": "PT15M33S",
        "video_views": 99,
        "likes_count": 7,
        "comments_count": 3,
    }

STAGING_ROW_02 = {
        "Video_ID": "abc123",
        "Video_Title": "New Title",
        "Upload_Date": "2026-01-01T00:00:00Z",
        "Duration": "PT15M33S",
        "Video_Views": 99,
        "Likes_Count": 7,
        "Comments_Count": 3,
    }

CORE_PARAMS_02 = {
        "video_id": "abc123",
        "upload_date": "2026-01-01T00:00:00Z",
        "video_title": "New Title",
        "duration": timedelta(seconds=933),
        "video_type": "Normal",
        "video_views": 99,
        "likes_count": 7,
        "comments_count": 3,
    }


def test_insert_rows_staging_builds_params_and_executes(cur):
    row = RAW_ROW_01
    insert_rows(cur, schema="staging", table="yt_api", row=row)

    cur.execute.assert_called_once()
    query_arg, params_arg = cur.execute.call_args[0]
    assert params_arg == STAGING_PARAMS_01

def test_insert_rows_core_builds_params_and_executes(cur):
    row = transform_duration(STAGING_ROW_01)
    insert_rows(cur, schema="core", table="yt_api", row=row)

    cur.execute.assert_called_once()
    query_arg, params_arg = cur.execute.call_args[0]
    print(params_arg)

    assert params_arg == CORE_PARAMS_01

def test_update_rows_staging_builds_params_and_executes(cur):
    row = RAW_ROW_02
    update_rows(cur, schema="staging", table="yt_api", row=row)

    cur.execute.assert_called_once()
    _, params_arg = cur.execute.call_args[0]
    assert params_arg == STAGING_PARAMS_02

def test_update_rows_core_builds_params_and_executes(cur):
    row = transform_duration(STAGING_ROW_02)
    update_rows(cur, schema="core", table="yt_api", row=row)

    cur.execute.assert_called_once()
    _, params_arg = cur.execute.call_args[0]
    assert params_arg == CORE_PARAMS_02

def test_delete_rows_passes_ids_as_single_execute_param(cur):
    ids = ["a", "b", "c"]

    delete_rows(cur, schema="staging", table="yt_api", ids_to_delete=ids)

    cur.execute.assert_called_once()
    query_arg, params_tuple = cur.execute.call_args[0]

    # execute second arg should be a 1-tuple containing the list
    assert isinstance(params_tuple, tuple)
    assert len(params_tuple) == 1
    assert params_tuple[0] == ids


def test_insert_rows_reraises_psycopg2_error(cur):
    row = RAW_ROW_01

    cur.execute.side_effect = Error("db error")

    with pytest.raises(Error):
        insert_rows(cur, schema="staging", table="yt_api", row=row)


def test_update_rows_reraises_psycopg2_error(cur):
    row = RAW_ROW_02
    cur.execute.side_effect = Error("db error")

    with pytest.raises(Error):
        update_rows(cur, schema="staging", table="yt_api", row=row)

