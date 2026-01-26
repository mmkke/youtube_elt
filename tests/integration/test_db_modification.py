import pytest
from psycopg2 import sql

from datawarehouse.data_utils import create_table
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows

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

# @pytest.mark.integration
def test_insert_update_delete_roundtrip(db):
    conn, cur, schema = db
    table = "yt_api"
    layer = "staging"

    create_table(cur, schema, layer, table)  # should create in schema
    conn.commit()

    insert_rows(cur, schema, layer, table, RAW_ROW)
    conn.commit()

    cur.execute(
        sql.SQL('SELECT "Video_Title", "Video_Views" FROM {}.{} WHERE "Video_ID"=%s')
        .format(sql.Identifier(schema), sql.Identifier(table)),
        ("abc123",),
    )
    row = cur.fetchone()
    assert row["Video_Title"] == "My Title"
    assert row["Video_Views"] == 10

    update_rows(cur, schema, layer, table, RAW_ROW_UPDATED)
    conn.commit()

    cur.execute(
        sql.SQL('SELECT "Video_Title", "Video_Views" FROM {}.{} WHERE "Video_ID"=%s')
        .format(sql.Identifier(schema), sql.Identifier(table)),
        ("abc123",),
    )
    row = cur.fetchone()
    assert row["Video_Title"] == "New Title"
    assert row["Video_Views"] == 99

    delete_rows(cur, schema, table, ["abc123"])
    conn.commit()

    cur.execute(
        sql.SQL('SELECT COUNT(*) AS n FROM {}.{} WHERE "Video_ID"=%s')
        .format(sql.Identifier(schema), sql.Identifier(table)),
        ("abc123",),
    )
    assert cur.fetchone()["n"] == 0