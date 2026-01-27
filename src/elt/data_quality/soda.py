import logging
from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)

SODA_PATH = "/opt/airflow/include/soda"
DATASOURCE = "pg_datasource"

def yt_elt_data_quality(schema):
    """
    Create a Soda data quality check task for a given schema.

    Parameters
    ----------
    schema : str
        Target schema name to inject into Soda checks.

    Returns
    -------
    BashOperator
        Airflow task that runs Soda CLI checks.
    """

    return BashOperator(
                            task_id=f"soda_test_{schema}",
                            bash_command=(
                                "soda scan "
                                "-d {{ params.datasource }} "
                                "-c {{ params.soda_path }}/configuration.yml "
                                "-v SCHEMA={{ params.schema }} "
                                "{{ params.soda_path }}/checks.yml"
                            ),
                            params={
                                "schema": schema,
                                "datasource": DATASOURCE,
                                "soda_path": SODA_PATH,
                            },
                        )