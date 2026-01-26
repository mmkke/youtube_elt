import pytest
from psycopg2 import sql
from datetime import timedelta

from datawarehouse.data_utils import create_table
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_transformations import transform_duration

RAW_ROW = {
    "video_id": "abc123",
    "title": "My Title",
    "publishedAt": "2026-01-01T00:00:00Z",
    "duration": "PT15M33S",
    "viewCount": 10,
    "likeCount": 2,
    "commentCount": 1,
}

RAW_ROW_UPDATED = {
    "video_id": "abc123",
    "title": "New Title",
    "publishedAt": "2026-01-01T00:00:00Z",
    "duration": "PT15M33S",
    "viewCount": 99,
    "likeCount": 7,
    "commentCount": 3,
}


RAW_ROW_1 = {
    "video_id": "abc123",
    "title": "My Title",
    "publishedAt": "2026-01-01T00:00:00Z",
    "duration": "PT15M33S",
    "viewCount": 10,
    "likeCount": 2,
    "commentCount": 1,
}

RAW_ROW_1_UPDATED = {
    "video_id": "abc123",
    "title": "My Title (Updated)",
    "publishedAt": "2026-01-01T00:00:00Z",
    "duration": "PT15M33S",
    "viewCount": 99,
    "likeCount": 7,
    "commentCount": 3,
}


def _fetch_one(cur, schema, table, video_id, layer="staging"):
    cols = [
        '"Video_ID"', '"Video_Title"', '"Upload_Date"', '"Duration"',
        '"Video_Views"', '"Likes_Count"', '"Comments_Count"',
    ]
    if layer == "core":
        cols.insert(4, '"Video_Type"')

    q = sql.SQL("""
        SELECT {cols}
        FROM {schema}.{table}
        WHERE "Video_ID" = %s;
    """).format(
        cols=sql.SQL(", ").join(sql.SQL(c) for c in cols),
        schema=sql.Identifier(schema),
        table=sql.Identifier(table),
    )
    cur.execute(q, (video_id,))
    return cur.fetchone()


def _count(cur, schema: str, table: str) -> int:
    q = sql.SQL('SELECT COUNT(*) AS n FROM {schema}.{table}').format(
        schema=sql.Identifier(schema),
        table=sql.Identifier(table),
    )
    cur.execute(q)
    return cur.fetchone()["n"]


def test_01_create_table_creates_schema_table(db):
    """
    Integration: DDL runs and table exists in Postgres in the ephemeral test schema.
    """
    conn, cur, schema = db
    table = "yt_api"
    layer = "staging"

    create_table(cur, schema, layer, table)
    conn.commit()

    cur.execute("SELECT to_regclass(%s) AS reg;", (f"{schema}.{table}",))
    row = cur.fetchone()
    assert row["reg"] == f"{schema}.{table}"


def test_02_insert_rows_persists_row(db):
    """
    Integration: insert_rows writes a record you can SELECT back.
    """
    conn, cur, schema = db
    table = "yt_api"
    layer = "staging"

    create_table(cur, schema, layer, table)
    conn.commit()

    insert_rows(cur, schema, layer, table, RAW_ROW_1)
    conn.commit()

    assert _count(cur, schema, table) == 1
    row = _fetch_one(cur, schema, table, "abc123")
    assert row is not None
    assert row["Video_ID"] == "abc123"
    assert row["Video_Title"] == "My Title"
    assert int(row["Video_Views"]) == 10


def test_03_update_rows_updates_existing_row(db):
    """
    Integration: update_rows modifies the stored row.
    """
    conn, cur, schema = db
    table = "yt_api"
    layer = "staging"

    create_table(cur, schema, layer, table)
    conn.commit()

    insert_rows(cur, schema, layer, table, RAW_ROW_1)
    conn.commit()

    update_rows(cur, schema, layer, table, RAW_ROW_1_UPDATED)
    conn.commit()

    row = _fetch_one(cur, schema, table, "abc123")
    assert row["Video_Title"] == "My Title (Updated)"
    assert int(row["Video_Views"]) == 99


def test_04_delete_rows_deletes_only_requested_ids(db):
    """
    Integration: delete_rows removes only the specified IDs.
    """
    conn, cur, schema = db
    table = "yt_api"
    layer = "staging"

    create_table(cur, schema, layer, table)
    conn.commit()

    r2 = dict(RAW_ROW_1)
    r2["video_id"] = "def456"
    r2["title"] = "Second"

    insert_rows(cur, schema, layer, table, RAW_ROW_1)
    insert_rows(cur, schema, layer, table, r2)
    conn.commit()

    delete_rows(cur, schema, table, ["abc123"])
    conn.commit()

    assert _fetch_one(cur, schema, table, "abc123") is None
    assert _fetch_one(cur, schema, table, "def456") is not None
    assert _count(cur, schema, table) == 1


def test_05_transform_duration_then_insert_interval_roundtrip(db):
    """
    Integration: transform_duration produces a timedelta + Video_Type, and the result
    inserts cleanly into a core-style table with INTERVAL Duration.
    """
    conn, cur, schema = db
    table = "yt_api"
    layer = "core"

    create_table(cur, schema, layer, table)
    conn.commit()

    # staging->core-like dict: match what your core insert expects
    core_like = {
        "Video_ID": RAW_ROW_1["video_id"],
        "Video_Title": RAW_ROW_1["title"],
        "Upload_Date": RAW_ROW_1["publishedAt"],
        "Duration": RAW_ROW_1["duration"],  # ISO 8601 string input for transform
        "Video_Views": RAW_ROW_1["viewCount"],
        "Likes_Count": RAW_ROW_1["likeCount"],
        "Comments_Count": RAW_ROW_1["commentCount"],
    }

    transformed = transform_duration(core_like)

    assert isinstance(transformed["Duration"], timedelta)
    assert transformed["Video_Type"] in ("Shorts", "Normal")

    insert_rows(cur, schema, layer, table, transformed)
    conn.commit()

    row = _fetch_one(cur, schema, table, "abc123", layer="core")
    assert row is not None
    assert row["Video_ID"] == "abc123"
    assert row["Duration"] is not None
    assert int(row["Video_Views"]) == 10
    assert row["Duration"] == timedelta(minutes=15, seconds=33)
    assert row["Video_Type"] == "Normal"

def test_06_insert_update_delete_roundtrip(db):
    conn, cur, schema = db
    table = "yt_api"
    layer = "staging"

    create_table(cur=cur, schema=schema, layer=layer, table=table)
    conn.commit()

    insert_rows(cur=cur, schema=schema, layer=layer, table=table, row=RAW_ROW)
    conn.commit()

    cur.execute(
        sql.SQL('SELECT "Video_Title", "Video_Views" FROM {}.{} WHERE "Video_ID"=%s')
        .format(sql.Identifier(schema), sql.Identifier(table)),
        ("abc123",),
    )
    row = cur.fetchone()
    assert row["Video_Title"] == "My Title"
    assert row["Video_Views"] == 10

    update_rows(cur=cur, schema=schema, layer=layer, table=table, row=RAW_ROW_UPDATED)
    conn.commit()

    cur.execute(
        sql.SQL('SELECT "Video_Title", "Video_Views" FROM {}.{} WHERE "Video_ID"=%s')
        .format(sql.Identifier(schema), sql.Identifier(table)),
        ("abc123",),
    )
    row = cur.fetchone()
    assert row["Video_Title"] == "New Title"
    assert row["Video_Views"] == 99

    delete_rows(cur=cur, schema=schema, table=table, ids_to_delete=["abc123"])
    conn.commit()

    cur.execute(
        sql.SQL('SELECT COUNT(*) AS n FROM {}.{} WHERE "Video_ID"=%s')
        .format(sql.Identifier(schema), sql.Identifier(table)),
        ("abc123",),
    )
    assert cur.fetchone()["n"] == 0