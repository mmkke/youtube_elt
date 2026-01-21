# Libraries
import logging
from airflow.decorators import task
from psycopg2 import sql, Error

# Modules
from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor, create_schema, create_table, get_video_ids
from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_transformations import transform_duration

# Params
logger = logging.getLogger(__name__)
TABLE = "yt_api"

# Tasks
@task
def staging_table():
    """
    Populate and maintain the staging YouTube API table.

    This task performs an incremental synchronization between the raw
    YouTube API JSON data and the `staging.yt_api` table. It ensures that
    the staging table always reflects the current state of the source
    API data.

    Workflow
    --------
    1. Ensure the `staging` schema and `yt_api` table exist.
    2. Load raw video metadata from the JSON ingestion output.
    3. Insert new videos not yet present in the staging table.
    4. Update existing videos whose metadata may have changed.
    5. Delete videos that exist in the staging table but are no longer
       present in the source JSON.
    6. Commit all changes as a single transaction.

    Data Characteristics
    --------------------
    - The staging table preserves raw API values (e.g., ISO 8601
      duration strings).
    - No semantic transformations are applied at this layer.
    - The table is treated as a faithful, mutable mirror of the API.

    Error Handling
    --------------
    - Any database error triggers a full rollback of the transaction.
    - Errors are logged with schema and table context.
    - Exceptions are re-raised to fail the Airflow task explicitly.

    Idempotency
    -----------
    This task is idempotent: re-running it with the same JSON input
    will not introduce duplicate rows or inconsistent state.

    Returns
    -------
    None
    """

    schema = "staging"
    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()
        create_schema(schema)
        create_table(schema, TABLE)

        # Load raw data from JSON
        raw_data = load_data()

        # Get video ids currently in the 'staging' table
        table_ids = set(get_video_ids(cur, schema, TABLE))
        
        # Upsert rows in the raw data
        for row in raw_data:
            video_id = row["video_id"]
            if not video_id:
                logger.warning("skipping row missing video_id: %s", row)
                continue
            # Update if exists, otherwise insert and add to table_ids set
            if video_id in table_ids:
                update_rows(cur, schema, TABLE, row)
            else:
                insert_rows(cur, schema, TABLE, row)
                table_ids.add(video_id)

        # Delete any rows in DB that are no longer present i the JSON
        ids_in_json = {row["video_id"] for row in raw_data if row.get("video_id")}
        ids_to_delete = list(table_ids - ids_in_json)
        if ids_to_delete:
            delete_rows(cur, schema, TABLE, ids_to_delete)
        
        conn.commit()
        logger.info("%s.%s update completed.", schema, TABLE)

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

    This task synchronizes data from the `staging.yt_api` table into the
    `core.yt_api` table, applying domain-level transformations and
    enforcing stricter schema constraints.

    Workflow
    --------
    1. Ensure the `core` schema and `yt_api` table exist.
    2. Read all rows from the staging table.
    3. Transform fields as needed for analytics use:
       - Convert ISO 8601 durations to PostgreSQL INTERVAL values.
       - Populate required core-only fields (e.g., Video_Type).
    4. Insert new records into the core table.
    5. Update existing records when metadata changes.
    6. Remove records from the core table that no longer exist in staging.
    7. Commit all changes as a single transaction.

    Data Characteristics
    --------------------
    - The core table enforces typed, analytics-ready columns.
    - Duration values are stored as PostgreSQL INTERVALs.
    - The table represents the canonical, cleaned dataset for downstream
      reporting and dashboards.

    Error Handling
    --------------
    - Any database error triggers a rollback of the entire transaction.
    - Errors are logged with schema and table context.
    - Exceptions are re-raised so Airflow marks the task as failed.

    Idempotency
    -----------
    This task is idempotent: running it multiple times without changes
    in the staging table will not modify the core table state.

    Returns
    -------
    None
    """
    schema = "core"
    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()
        create_schema(schema)
        create_table(schema, TABLE)

        # Get table ids from 'core' table and init staging_ids set
        table_ids = set(get_video_ids(cur, schema, TABLE))
        staging_ids = set()

        # Get rows from 'staging' table 
        fetch_rows_sql = (
                            sql.SQL("""
                                    SELECT
                                        "Video_ID",
                                        "Upload_Date",
                                        "Video_Title",
                                        "Duration",
                                        "Video_Views",
                                        "Likes_Count",
                                        "Comments_Count"
                                    FROM {schema}.{table};
                                    """).
                            format(
                                    schema=sql.Identifier("staging"),
                                    table=sql.Identifier(TABLE))
                        )
        cur.execute(fetch_rows_sql)
        rows = cur.fetchall()

        # Upsert through rows
        for row in rows:
            video_id = row["Video_ID"]
            if not video_id:
                logger.warning("skipping row missing video_id: %s", row)
                continue
            staging_ids.add(video_id)
            transformed_row = transform_duration(row)
            # If video_id already exists in 'core' update the row, otherwise insert new row
            if video_id in table_ids:
                update_rows(cur, schema, TABLE, transformed_row)
            else:
                insert_rows(cur, schema, TABLE, transformed_row)
                table_ids.add(video_id)

        # Delete any rows from 'core' that no longer appear in 'staging' table
        ids_to_delete = list(table_ids - staging_ids)
        if ids_to_delete:
            delete_rows(cur, schema, TABLE, ids_to_delete)
        
        conn.commit()
        logger.info("%s.%s update completed.", schema, TABLE)

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