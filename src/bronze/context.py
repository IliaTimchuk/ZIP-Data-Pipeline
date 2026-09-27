import os
import logging
import pyarrow as pa
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def get_context() -> dict:
    try:
        context = {
            "landing_bucket": os.environ["LANDING_BUCKET"],
            "landing_key": os.environ["LANDING_KEY"],
            "dataset_name": os.environ["DATASET_NAME"],
            "destination_bucket": os.environ["DESTINATION_BUCKET"],
            "aws_endpoint_url": os.getenv("AWS_ENDPOINT"),
        }
        logger.info("The bronze context was succesfully extracted. Values: %s", context)
        return context
    except KeyError as e:
        raise KeyError(
            f"The required environment variable(-s) was not provided: {e}"
        ) from e


def get_airflow_metadata_columns(landing_key: str):
    time = datetime.now(timezone.utc)
    bronze_processed_at = int(time.timestamp() * 1000)
    
    dag_run_id = os.environ["_DAG_RUN_ID"]

    zip_file_name = os.path.basename(landing_key)

    airflow_metadata_columns = {
        "_bronze_processed_at": pa.scalar(
            bronze_processed_at, pa.timestamp("ms", tz="UTC")
        ),
        "_dag_run_id": pa.scalar(dag_run_id, pa.string()),
        "_zip_file_name": pa.scalar(zip_file_name, pa.string()),
    }

    logger.info(
        "The airflow_metadata_columns were succesfully built. Values: %s",
        airflow_metadata_columns,
    )

    return airflow_metadata_columns
