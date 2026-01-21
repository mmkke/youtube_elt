# Libraries
from isodate import parse_duration 
from datetime import timedelta


# Functions
# def parse_duration_str(iso_str):
#     """COnverts time format from ISO 8601 duration (P#DT#H#M#S) to string formatted as MM:HH:SS"""
#     td = parse_duration(iso_str)
#     total_seconds = td.total_seconds()

#     # Handle isodate.Duration vs timedelta
#     if hasattr(td, "totimedelta"):
#         td = td.totimedelta()

#     hours, remainder = divmod(total_seconds, 3600)
#     minutes, seconds = divmod(remainder, 60)
#     return  f"{hours:02}:{minutes:02}:{seconds:02}"


# def transform_duration(row: dict) -> dict:
#     """
#     Transform ISO 8601 Duration field into HH:MM:SS string.
#     """
#     iso_str = row.get("Duration")
#     if not iso_str:
#         return row
#     row["Duration"] = parse_duration(iso_str)
#     return row


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