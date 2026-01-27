import pytest
from unittest.mock import Mock
from psycopg2 import Error, sql


from elt.dwh.data_utils import get_video_ids

@pytest.fixture
def cur():
    return Mock()

def test_get_video_ids_executes_select_and_returns_ids(cur):
    # Assign return value for .fetchall() method
    cur.fetchall.return_value = [
        {"Video_ID": "a1"},
        {"Video_ID": "b2"},
        {"Video_ID": "c3"},
    ]
    out = get_video_ids(cur, schema="staging", table="yt_api")

   
    assert out == ["a1", "b2", "c3"]
    cur.execute.assert_called_once()
    cur.fetchall.assert_called_once()
    query_arg = cur.execute.call_args[0][0]
    assert isinstance(query_arg, (sql.Composed, sql.SQL))

def test_get_video_ids_empty_returns_empty_list(cur):
    cur.fetchall.return_value = []
    out = get_video_ids(cur, schema="staging", table='yt_api')

    assert out == []
    cur.execute.assert_called_once()
    cur.fetchall.assert_called_once()
    query_arg = cur.execute.call_args[0][0]
    assert isinstance(query_arg, (sql.Composed, sql.SQL))