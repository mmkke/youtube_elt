
# Libraries
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

# Modules
from elt.api.extract_functions import (
                                    get_playlist_id, 
                                    get_video_ids, 
                                    extract_video_detail, 
                                    save_video_data_to_json
                                    )
from elt.dwh.tasks import(
                                staging_table,
                                core_table
                                )

from elt.data_quality.soda import yt_elt_data_quality
# ============================================================
# Define local time zone
local_tz = pendulum.timezone("America/New_York")

# Default Args
default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "data@engineers.com",
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2026, 1, 1, tzinfo=local_tz),
    # 'end_date': datetime(2030, 12, 31, tzinfo=local_tz),
}

# ============================================================
## dag_01: Accessing YouTube API and saving JSON with raw video data

with DAG(
        dag_id="youtube_api_ingestion",
        default_args=default_args,
        description= "This DAG handles API ingestion and writes raw JSON to storage.",
        schedule = "0 8 * * *", # cron code "minute hour day month weekday" https://crontab.guru/#0_8_*_*_*
        catchup = False,
    ) as dag_01:
    
    # Params
    CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")
    json_path = f"data/{CHANNEL_HANDLE}"
    max_result = 50
    batch_size = 50

    # Tasks
    playlistID = get_playlist_id(channel_handle=CHANNEL_HANDLE)
    video_ids = get_video_ids(
                        playlistID, 
                        max_results=max_result
                        )
    extracted_data = extract_video_detail(
                                    video_ids=video_ids,
                                    batch_size=batch_size
                                    )
    save_to_json_task = save_video_data_to_json(
                        extracted_data=extracted_data,
                        path=json_path
                        )
    
    # Define dependencies
    playlistID >> video_ids >> extracted_data >> save_to_json_task

# ============================================================
## dag_02: Loading data into 'staging' and 'core' schemas.

with DAG(
        dag_id="youtube_db_load",
        default_args=default_args,
        description= "This DAG handles data loading to 'staging' and 'core' schemas.",
        schedule = "0 10 * * *", # cron code "minute hour day month weekday" https://crontab.guru/#0_8_*_*_*
        catchup = False,
    ) as dag_02:

    # Tasks
    update_staging_layer = staging_table()
    update_core_layer = core_table()

    #Define dependencies
    update_staging_layer >> update_core_layer


# ============================================================
## dag_02: Loading data into 'staging' and 'core' schemas.

with DAG(
        dag_id="data_quality_soda_check",
        default_args=default_args,
        description= "This DAG checks data quality on both staging and core layers using Soda.",
        schedule = "0 12 * * *", # cron code "minute hour day month weekday" https://crontab.guru/#0_8_*_*_*
        catchup = False,
    ) as dag_03:

    # Tasks
    soda_validate_staging = yt_elt_data_quality(schema='staging')
    soda_validate_core = yt_elt_data_quality(schema='core')

    #Define dependencies
    soda_validate_staging >> soda_validate_core