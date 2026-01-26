import os
import pytest 
from unittest import mock
from airflow.models import Variable
import sys
from pathlib import Path

DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"  # /opt/airflow/dags
sys.path.insert(0, str(DAGS_DIR))

@pytest.fixture
def api_key():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_API_KEY="MOCK_KEY1234"):
        yield Variable.get("API_KEY")