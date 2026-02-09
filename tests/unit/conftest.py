from unittest import mock

import pytest
from airflow.models import Variable

# DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"  # /opt/airflow/dags
# sys.path.insert(0, str(DAGS_DIR))


@pytest.fixture
def api_key():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_API_KEY="MOCK_KEY1234"):
        yield Variable.get("API_KEY")
