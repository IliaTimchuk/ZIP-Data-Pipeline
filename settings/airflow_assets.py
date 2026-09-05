import settings.pipeline_config as conf
from airflow.sdk import Asset

# is used to trigger the bronze_zip_to_parquet DAG (dags/bronze.py) from the ingest_from_s3 DAG (dags/ingestion.py)
LANDING_ASSET = Asset(uri=f"s3://{conf.LANDING_BUCKET}")