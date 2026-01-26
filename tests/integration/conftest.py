import sys
import uuid
import pytest
from psycopg2 import sql
from pathlib import Path 

DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"  # /opt/airflow/dags
sys.path.insert(0, str(DAGS_DIR))

from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor

TEST_CONN_ID = "postgres_db_yt_elt_test"
DATABASE = "elt_test_db"
@pytest.fixture
def db():
    """
    Yields (conn, cur, schema) connected to the TEST database.
    Creates a unique schema for the test and drops it afterwards.
    """
    schema = f"test_{uuid.uuid4().hex[:10]}"
    conn, cur = get_conn_cursor(conn_id=TEST_CONN_ID, database=DATABASE)

    try:
        cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        conn.commit()
        yield conn, cur, schema
    finally:
        # best-effort cleanup
        try:
            cur.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
            conn.commit()
        except Exception:
            conn.rollback()
        close_conn_cursor(conn, cur)