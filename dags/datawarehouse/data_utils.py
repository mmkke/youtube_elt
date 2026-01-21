
# Libraries

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql
from psycop2.extras import RealDictCursor

table = "yt_api"


def get_conn_cursor():
    """Initializes connection and cursor for Database"""
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur

def close_conn_cursor(conn, cur):
    """Closes connection and cursor"""
    cur.close()
    conn.close()


def create_schema(schema):
    """Create schema"""
    conn, cur = get_conn_cursor()
    
    schema_ddl = (
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema}").
                    format(sql.Identifier(schema))
                  )
    cur.execute(schema_ddl)
    conn.commit()

    close_conn_cursor(conn, cur)

def create_table(schema: str, table: str) -> None:
    """Create a table if it does not exist."""
    conn, cur = get_conn_cursor()

    try:
        if schema == "staging":
            ddl = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" VARCHAR(20) NOT NULL,
                    "Video_Views" BIGINT,
                    "Likes_Count" BIGINT,
                    "Comments_Count" BIGINT
                );
            """)
        else:
            ddl = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" INTERVAL NOT NULL,
                    "Video_Type" VARCHAR(10),
                    "Video_Views" BIGINT,
                    "Likes_Count" BIGINT,
                    "Comments_Count" BIGINT
                );
            """)

        cur.execute(
                    ddl.format(
                            schema=sql.Identifier(schema),
                            table=sql.Identifier(table),
                        )
                    )
        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        close_conn_cursor(conn, cur)


def get_video_ids(cur, schema: str, table: str) -> list[str]:
    """Return list of video IDs from the table."""
    query = (
            sql.SQL(
                """
                    SELECT "Video_ID"
                    FROM {schema}.{table};
                """).
            format(
                    schema=sql.Identifier(schema), 
                    table=sql.Identifier(table))
            )

    cur.execute(query)
    rows = cur.fetchall()

    return [row["Video_ID"] for row in rows]
