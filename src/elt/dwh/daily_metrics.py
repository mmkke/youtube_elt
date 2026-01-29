from psycopg2 import sql
from datetime import date
from typing import Optional

def create_daily_metrics_table(
                            cur,
                            schema: str = "core",
                            table: str = "yt_api_metrics_daily",
                            parent_schema: str = "core",
                            parent_table: str = "yt_api",
                        ) -> None:
    """Create the Daily Metrics table."""
    ddl = (sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {schema}.{table} (
                        "Video_ID"       VARCHAR(11) NOT NULL,
                        "Snapshot_Date"  DATE NOT NULL,
                        "Video_Views"    BIGINT,
                        "Likes_Count"    BIGINT,
                        "Comments_Count" BIGINT,
                        "Ingested_At"    TIMESTAMPTZ NOT NULL DEFAULT now(),
                        PRIMARY KEY ("Video_ID", "Snapshot_Date"),
                        FOREIGN KEY ("Video_ID")
                            REFERENCES {parent_schema}.{parent_table} ("Video_ID")
                            ON DELETE CASCADE
                    );
                """).
        format(
                schema=sql.Identifier(schema),
                table=sql.Identifier(table),
                parent_schema=sql.Identifier(parent_schema),
                parent_table=sql.Identifier(parent_table),
            ))

    cur.execute(ddl)

def create_daily_metrics_indexes(cur, schema: str = "core", table: str = "yt_api_metrics_daily") -> None:
    """Create indexes for the daily metrics table."""
    idx_date = (sql.SQL("""
                            CREATE INDEX IF NOT EXISTS {idx}
                            ON {schema}.{table} ("Snapshot_Date");
                        """).
                format(
                        idx=sql.Identifier(f"idx_{table}_snapshot_date"),
                        schema=sql.Identifier(schema),
                        table=sql.Identifier(table),
                    ))

    idx_video = sql.SQL("""
        CREATE INDEX IF NOT EXISTS {idx}
        ON {schema}.{table} ("Video_ID");
    """).format(
        idx=sql.Identifier(f"idx_{table}_video_id"),
        schema=sql.Identifier(schema),
        table=sql.Identifier(table),
    )

    cur.execute(idx_date)
    cur.execute(idx_video)



def upsert_daily_metrics(
                        cur,
                        row: dict,
                        schema: str = "core",
                        table: str = "yt_api_metrics_daily",
                        snapshot_date: Optional[date] = None,
                    ) -> None:
    """
    Insert/update one daily metrics row.
    Default snapshot_date is today (CURRENT_DATE equivalent).
    """
    params = {
        "video_id": row["Video_ID"],
        "snapshot_date": snapshot_date or date.today(),
        "video_views": row.get("Video_Views"),
        "likes_count": row.get("Likes_Count"),
        "comments_count": row.get("Comments_Count"),
    }

    query = (sql.SQL("""
                        INSERT INTO {schema}.{table} (
                            "Video_ID",
                            "Snapshot_Date",
                            "Video_Views",
                            "Likes_Count",
                            "Comments_Count"
                        )
                        VALUES (
                            %(video_id)s,
                            %(snapshot_date)s,
                            %(video_views)s,
                            %(likes_count)s,
                            %(comments_count)s
                        )
                        ON CONFLICT ("Video_ID", "Snapshot_Date")
                        DO UPDATE SET
                            "Video_Views"     = EXCLUDED."Video_Views",
                            "Likes_Count"     = EXCLUDED."Likes_Count",
                            "Comments_Count"  = EXCLUDED."Comments_Count"
                        ;
                    """).
                format(
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(table),
                ))

    cur.execute(query, params)