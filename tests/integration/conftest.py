import os
import sys
import uuid
import pytest
from pathlib import Path
from psycopg2 import sql

# Ensure /opt/airflow/dags is importable inside the container
DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"
sys.path.insert(0, str(DAGS_DIR))

from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor

TEST_CONN_ID = os.getenv("TEST_CONN_ID", "postgres_db_yt_elt_test")
TEST_DATABASE = os.getenv("TEST_DATABASE", "elt_test_db")


@pytest.fixture
def db():
    """
    Integration DB fixture.

    - Connects to the configured TEST database (via Airflow Connection ID).
    - Creates a unique schema per test for isolation.
    - Yields (conn, cur, schema).
    - Drops the schema CASCADE after the test (best-effort).
    """
    schema = f"test_{uuid.uuid4().hex[:10]}"
    conn, cur = get_conn_cursor(conn_id=TEST_CONN_ID, database=TEST_DATABASE)

    try:
        cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        conn.commit()
        yield conn, cur, schema
    finally:
        try:
            cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
        close_conn_cursor(conn, cur)