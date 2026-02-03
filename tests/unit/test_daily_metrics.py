from unittest.mock import Mock
from datetime import date
import time_machine
import pendulum

from elt.dwh.daily_metrics import create_daily_metrics_table, create_daily_metrics_indexes, upsert_daily_metrics

import elt.dwh.tasks as tasks



def test_01_create_daily_metrics_table_executes_ddl():
    cur = Mock()
    create_daily_metrics_table(cur, schema="core", table="yt_api_metrics_daily")
    cur.execute.assert_called_once()

def test_02_create_daily_metrics_indexes_executes_ddl():
    cur = Mock()
    create_daily_metrics_indexes(cur, schema="core", table="yt_api_metrics_daily")
    assert cur.execute.call_count == 2

def test_03_upsert_daily_metrics_executes_ddl():
    cur = Mock()
    row = {
        "Video_ID": "abc123",
        "Video_Views": 4,
        "Like_Count": 4,
        "Comments_Count": 4
    }
    upsert_daily_metrics(cur, row, schema="core", table="yt_api_metrics_daily")
    cur.execute.assert_called_once()

@time_machine.travel("2026-01-27")
def test_04_upsert_daily_metrics_defaults_snapshot_date_to_today():
    cur = Mock()
    row = {"Video_ID": "abc123", "Video_Views": 10, "Likes_Count": 2, "Comments_Count": 1}

    upsert_daily_metrics(cur, row)

    _, params = cur.execute.call_args[0]
    assert params["snapshot_date"] == date(2026, 1, 27)


def test_05_daily_metrics_table_uses_logical_date(monkeypatch):
    conn = Mock()
    cur = Mock()
    cur.fetchall.return_value = [
        {"Video_ID": "abc123", "Video_Views": 10, "Likes_Count": 2, "Comments_Count": 1},
        {"Video_ID": "def456", "Video_Views": 20, "Likes_Count": 4, "Comments_Count": 0},
    ]

    # Patch external dependencies in *the module where they’re used*
    monkeypatch.setattr(tasks, "get_conn_cursor", lambda: (conn, cur))
    monkeypatch.setattr(tasks, "create_daily_metrics_table", Mock())
    monkeypatch.setattr(tasks, "create_daily_metrics_indexes", Mock())
    upsert_mock = Mock()
    monkeypatch.setattr(tasks, "upsert_daily_metrics", upsert_mock)
    monkeypatch.setattr(tasks, "close_conn_cursor", Mock())

    logical_date = pendulum.datetime(2026, 1, 27, tz="UTC")
    expected_snapshot_date = logical_date.date()

    # Act
    # Because @task wraps the function, call the underlying python callable.
    # In Airflow 2.x TaskFlow, this is typically available as .__wrapped__.
    tasks.daily_metrics_table.__wrapped__(logical_date=logical_date)

    # Assert
    # It should call upsert once per fetched row, and always with snapshot_date = logical_date.date()
    assert upsert_mock.call_count == 2
    for call in upsert_mock.call_args_list:
        _, kwargs = call
        assert kwargs["snapshot_date"] == expected_snapshot_date

    # Optional: ensure commit happened once at end
    conn.commit.assert_called()