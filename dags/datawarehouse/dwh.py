# Libraries
import logging
from airflow.decorators import task
from psycopg2 import sql, Error

# Modules
from data_utils import get_conn_cursor, close_conn_cursor, create_schema, create_table, get_video_ids
from data_loading import load_data
from data_modification import insert_rows, update_rows, delete_rows
from data_transformations import transform_duration

# Params
logger = logging.getLogger(__name__)
TABLE = "yt_api"

# Tasks
@task
def staging_table():

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
            id = row["video_id"]
            if not id:
                logger.warning("skipping row missing video_id: %s", row)
                continue
            # Update if exists, otherwise insert and add to table_ids set
            if id in table_ids:
                update_rows(cur, schema, TABLE, row)
            else:
                insert_rows(cur, schema, TABLE, row)
                table_ids.add(id)

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
                                            "video_id",
                                            "upload_date",
                                            "video_title",
                                            "video_views",
                                            "likes_count",
                                            "comments_count",
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
            id = row["video_id"]
            if not id:
                logger.warning("skipping row missing video_id: %s", row)
                continue
            staging_ids.add(id)
            transformed_row = transform_duration(row)
            # If id already exists in 'core' update the row, otherwise insert new row
            if id in table_ids:
                update_rows(cur, schema, TABLE, transformed_row)
            else:
                insert_rows(cur, schema, TABLE, transformed_row)
                table_ids.add(id)

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