
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows


def test_insert_update_delete_roundtrip(setup_db, conn, cur):
    row = {
        "video_id": "abc123",
        "title": "My Title",
        "publishedAt": "2026-01-01T00:00:00Z",
        "duration": "PT15M33S",
        "viewCount": 10,
        "likeCount": 2,
        "commentCount": 1,
    }

    insert_rows(cur, "staging", "yt_api", row)
    conn.commit()

    cur.execute('SELECT * FROM staging.yt_api WHERE "Video_ID"=%s', ("abc123",))
    db_row = cur.fetchone()
    assert db_row["Video_ID"] == "abc123"
    assert db_row["Video_Title"] == "My Title"

    # update same id
    row2 = dict(row)
    row2["title"] = "Updated Title"
    row2["viewCount"] = 999
    update_rows(cur, "staging", "yt_api", row2)
    conn.commit()

    cur.execute('SELECT * FROM staging.yt_api WHERE "Video_ID"=%s', ("abc123",))
    db_row = cur.fetchone()
    assert db_row["Video_Title"] == "Updated Title"
    assert db_row["Video_Views"] == 999

    # delete
    delete_rows(cur, "staging", "yt_api", ["abc123"])
    conn.commit()

    cur.execute('SELECT COUNT(*) AS n FROM staging.yt_api WHERE "Video_ID"=%s', ("abc123",))
    assert cur.fetchone()["n"] == 0