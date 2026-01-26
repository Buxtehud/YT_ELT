from airflow import DAG
from airflow.models import Variable
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta, date
import json
from api.video_stats import get_playlist_id,get_video_ids,extract_video_data, save_to_json
from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality



# Define the local timezone
local_tz = ZoneInfo("America/Buenos_Aires")

# # Default Args
default_args = {
    "owner":"andy",
    "depends_on_past": False,
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2026, 1, 1, tzinfo=local_tz),
    # "end_date": datetime(2030, 12, 31, tzinfo=local_tz)
}



with DAG(
    dag_id = 'produce_json',
    default_args = default_args,
    description = 'DAG to produce JSON file with raw data',
    schedule = '0 14 * * *',
    catchup = False
) as dag:
    
    # Define tasks
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)

    playlist_id >> video_ids >> extract_data >> save_to_json_task

with DAG(
    dag_id = 'update_db',
    default_args = default_args,
    description = 'DAG to process JSON file and insert data into both staging and core schemas',
    schedule = '0 15 * * *',
    catchup = False
) as dag:
    
    # Define tasks
    update_staging_table = staging_table()
    update_core_table = core_table()

    update_staging_table >> update_core_table

with DAG(
    dag_id = 'data_quality',
    default_args = default_args,
    description = 'DAG to check the data quality on both layers in the db',
    schedule = '0 16 * * *',
    catchup = False
) as dag:
    
    # Define tasks
    soda_validate_staging = yt_elt_data_quality("staging")
    soda_validate_core = yt_elt_data_quality("core")

    soda_validate_staging >> soda_validate_core