# Libraries
from isodate import parse_duration 
from datetime import timedelta

def transform_duration(row: dict) -> dict:
    """
    Transform ISO 8601 duration into datetime.timedelta
    (for PostgreSQL INTERVAL) and classify "Video_Type" as 
    either 'Shorts' if <= 60 seconds, or 'Normal.
    """
    iso_str = row.get("Duration")
    if not iso_str:
        return row
    duration = parse_duration(iso_str)

    # Normalize Duration → timedelta
    if hasattr(duration, "totimedelta"):
        duration = duration.totimedelta()

    row["Duration"] = duration
    row["Video_Type"] = "Shorts" if duration.total_seconds() <= 60 else "Normal"
    return row