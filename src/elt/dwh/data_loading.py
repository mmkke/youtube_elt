
# Libraries
import json
import logging
from datetime import date
from pathlib import Path
from airflow.models import Variable

logger = logging.getLogger(__name__)

def load_data():

    channel_handle = Variable.get("CHANNEL_HANDLE", default_var=None)
    if not channel_handle:
        logger.error("Missing required Airflow Variable: CHANNEL_HANDLE")
        raise RuntimeError("CHANNEL_HANDLE not set")
    
    file_path = Path("data") / f"{channel_handle}_{date.today()}.json"
    if not file_path.is_file():
        logger.error("JSON file not found at path=%s", file_path)
        raise FileNotFoundError(file_path)
    
    try:
        logger.info(f"Attempting to process file: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as raw_data:
            data = json.load(raw_data)
        return data 
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in file {file_path}: {e}")
        raise

