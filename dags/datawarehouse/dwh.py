"""
Airflow tasks for syncing YouTube API data into a Postgres warehouse.

This module defines two tasks:
- staging_table: sync raw JSON → staging.yt_api
- core_table: sync staging.yt_api → core.yt_api (with transformations)

Logging is structured to be Airflow-friendly:
- INFO for run-level summaries and counts
- DEBUG for per-row insert/update details (to avoid noisy logs at scale)
"""

# Libraries
import logging
from airflow.decorators import task
from psycopg2 import sql, Error

# Modules
from datawarehouse.data_utils import (
    get_conn_cursor,
    close_conn_cursor,
    create_schema,
    create_table,
    get_video_ids,
)
from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_transformations import transform_duration

# Params
logger = logging.getLogger(__name__)
TABLE = "yt_api"


@task
def staging_table():
    """
    Populate and maintain the staging YouTube API table.

    This task performs an incremental synchronization between the raw
    YouTube API JSON data and the `staging.yt_api` table.

    It upserts rows (update if exists, otherwise insert) and deletes rows
    that are no longer present in the JSON snapshot. All changes are
    committed as a single transaction for consistency. Any error triggers
    a rollback and the task fails.
    """
    schema = "staging"
    conn, cur = None, None

    # Counters for concise Airflow logs
    inserted = 0
    updated = 0
    skipped = 0
    deleted = 0

    try:
        logger.info("Starting sync for %s.%s", schema, TABLE)

        conn, cur = get_conn_cursor()

        # Ensure schema/table exist (note: these helpers may manage their own connections internally)
        create_schema(schema)
        create_table(schema, TABLE)

        # Load raw data from JSON
        raw_data = load_data()
        logger.info("Raw rows loaded from JSON: %d", len(raw_data))

        # IDs currently in the staging table
        table_ids = set(get_video_ids(cur, schema, TABLE))
        logger.info("Existing IDs in %s.%s: %d", schema, TABLE, len(table_ids))

        # Upsert rows from JSON
        for row in raw_data:
            video_id = row.get("video_id")
            if not video_id:
                skipped += 1
                logger.warning("Skipping row missing video_id: %s", row)
                continue

            if video_id in table_ids:
                update_rows(cur, schema, TABLE, row)
                updated += 1
                logger.debug("Updated Video_ID=%s in %s.%s", video_id, schema, TABLE)
            else:
                insert_rows(cur, schema, TABLE, row)
                inserted += 1
                table_ids.add(video_id)
                logger.debug("Inserted Video_ID=%s in %s.%s", video_id, schema, TABLE)

        # Delete rows that are no longer present in JSON
        ids_in_json = {r.get("video_id") for r in raw_data if r.get("video_id")}
        ids_to_delete = list(table_ids - ids_in_json)
        if ids_to_delete:
            delete_rows(cur, schema, TABLE, ids_to_delete)
            deleted = len(ids_to_delete)

        conn.commit()

        logger.info(
            "%s.%s sync complete: inserted=%d updated=%d deleted=%d skipped=%d total_after=%d",
            schema,
            TABLE,
            inserted,
            updated,
            deleted,
            skipped,
            len(table_ids) - deleted,  # total_after is best-effort; table_ids included deleted ids before delete calc
        )

    except Error:
        if conn:
            conn.rollback()
        logger.exception("DB error updating %s.%s", schema, TABLE)
        raise
    except Exception:
        if conn:
            conn.rollback()
        logger.exception("Unexpected error updating %s.%s", schema, TABLE)
        raise
    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)


@task
def core_table():
    """
    Populate and maintain the core YouTube analytics table.

    This task synchronizes rows from `staging.yt_api` into `core.yt_api`,
    applying transformations (e.g., ISO 8601 duration → INTERVAL) and
    enforcing stricter schema requirements.

    It upserts rows (update if exists, otherwise insert) and deletes rows
    from core that are no longer present in staging. All changes are
    committed as a single transaction for consistency. Any error triggers
    a rollback and the task fails.
    """
    schema = "core"
    conn, cur = None, None

    inserted = 0
    updated = 0
    skipped = 0
    deleted = 0

    try:
        logger.info("Starting sync for %s.%s", schema, TABLE)

        conn, cur = get_conn_cursor()

        # Ensure schema/table exist (note: these helpers may manage their own connections internally)
        create_schema(schema)
        create_table(schema, TABLE)

        # Existing IDs in core table
        table_ids = set(get_video_ids(cur, schema, TABLE))
        logger.info("Existing IDs in %s.%s: %d", schema, TABLE, len(table_ids))

        # Pull all rows from staging
        fetch_rows_sql = sql.SQL(
            """
            SELECT
                "Video_ID",
                "Upload_Date",
                "Video_Title",
                "Duration",
                "Video_Views",
                "Likes_Count",
                "Comments_Count"
            FROM {schema}.{table};
            """
        ).format(
            schema=sql.Identifier("staging"),
            table=sql.Identifier(TABLE),
        )

        cur.execute(fetch_rows_sql)
        rows = cur.fetchall()
        logger.info("Rows fetched from staging.%s: %d", TABLE, len(rows))

        # Track which IDs currently exist in staging, for delete detection
        staging_ids = set()

        # Upsert into core
        for row in rows:
            video_id = row.get("Video_ID")
            if not video_id:
                skipped += 1
                logger.warning("Skipping row missing Video_ID: %s", row)
                continue

            staging_ids.add(video_id)
            transformed_row = transform_duration(row)

            if video_id in table_ids:
                update_rows(cur, schema, TABLE, transformed_row)
                updated += 1
                logger.debug("Updated Video_ID=%s in %s.%s", video_id, schema, TABLE)
            else:
                insert_rows(cur, schema, TABLE, transformed_row)
                inserted += 1
                table_ids.add(video_id)
                logger.debug("Inserted Video_ID=%s in %s.%s", video_id, schema, TABLE)

        # Delete any rows from core that no longer appear in staging
        ids_to_delete = list(table_ids - staging_ids)
        if ids_to_delete:
            delete_rows(cur, schema, TABLE, ids_to_delete)
            deleted = len(ids_to_delete)

        conn.commit()

        logger.info(
            "%s.%s sync complete: inserted=%d updated=%d deleted=%d skipped=%d staging_rows=%d core_before=%d core_after≈%d",
            schema,
            TABLE,
            inserted,
            updated,
            deleted,
            skipped,
            len(staging_ids),
            len(table_ids) + deleted - inserted,  # best-effort: before adds/removes
            len(table_ids) - deleted,             # best-effort: after
        )

    except Error:
        if conn:
            conn.rollback()
        logger.exception("DB error updating %s.%s", schema, TABLE)
        raise
    except Exception:
        if conn:
            conn.rollback()
        logger.exception("Unexpected error updating %s.%s", schema, TABLE)
        raise
    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)