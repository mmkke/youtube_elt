
# Libraries
import logging
from psycopg2 import sql, Error


logger = logging.getLogger(__name__)
# table = "yt_api"

def insert_rows(cur, schema, table, row):
    try:
        if schema == "staging":
            params = {
                "video_id": row["video_id"],
                "video_title": row["title"],
                "upload_date": row["publishedAt"],
                "duration": row["duration"],
                "video_views": row["viewCount"],
                "likes_count": row["likeCount"],
                "comments_count": row["commentCount"],
            }
        else:
            params = {
                "video_id": row["Video_ID"],
                "video_title": row["Video_Title"],
                "upload_date": row["Upload_Date"],
                "duration": row["Duration"],
                "video_type": row["Video_Type"],
                "video_views": row["Video_Views"],
                "likes_count": row["Likes_Count"],
                "comments_count": row["Comments_Count"],
            }

        query = (
                sql.SQL(
                    """
                        INSERT INTO {schema}.{table} (
                            "Video_ID",
                            "Video_Title",
                            "Upload_Date",
                            "Duration",
                            "Video_Views",
                            "Likes_Count",
                            "Comments_Count"
                        )
                        VALUES (
                            %(video_id)s,
                            %(video_title)s,
                            %(upload_date)s,
                            %(duration)s,
                            %(video_views)s,
                            %(likes_count)s,
                            %(comments_count)s
                        )
                    """).
                format(
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(table),
                ))

        cur.execute(query, params)
        logger.info("Inserted row with Video_ID=%s", params["video_id"])

    except Error:
        logger.exception("Insert failed for video_id=%s", params.get("video_id"))
        raise
    
def update_rows(cur, schema, table, row):

    try:
        if schema == "staging":
            params = {
                "video_id": row["video_id"],
                "upload_date": row["publishedAt"],
                "video_title": row["title"],
                "duration": row["duration"],
                "video_views": row["viewCount"],
                "likes_count": row["likeCount"],
                "comments_count": row["commentCount"],
            }
        else:
            params = {
                "video_id": row["Video_ID"],
                "upload_date": row["Upload_Date"],
                "video_title": row["Video_Title"],
                "duration": row["Duration"],
                "video_type": row["Video_Type"],
                "video_views": row["Video_Views"],
                "likes_count": row["Likes_Count"],
                "comments_count": row["Comments_Count"],
            }

        query = (
                sql.SQL(
                    """
                        UPDATE {schema}.{table}
                        SET
                            "Video_Title"    = %(video_title)s,
                            "Video_Views"    = %(video_views)s,
                            "Likes_Count"    = %(likes_count)s,
                            "Comments_Count" = %(comments_count)s
                        WHERE
                            "Video_ID" = %(video_id)s
                            AND "Upload_Date" = %(upload_date)s
                    """).
                format(
                        schema=sql.Identifier(schema),
                        table=sql.Identifier(table),
                ))

        cur.execute(query, params)

    except Error:
        logger.exception("Update failed for video_id=%s", params.get("video_id"))
        raise

def delete_rows(cur, schema, table, ids_to_delete):
    "Deletes rows based on list of IDs"

    try:
        sql_string = (sql.SQL("""
                                DELETE FROM {schema}.{table}
                                WHERE "Video_ID" = ANY(%s)
                             """).
                        format(
                                schema=sql.Identifier(schema),
                                table = sql.Identifier(table)
                                )
                    )
        cur.execute(sql_string, (ids_to_delete,))
    except Error:
        logger.exception(f"Delete failed for Video_IDs=%s", ids_to_delete)
        raise