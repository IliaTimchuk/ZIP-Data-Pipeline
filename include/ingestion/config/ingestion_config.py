import os

LANDING_BUCKET = os.getenv("LANDING_BUCKET", "landing")

AWS_CONN_NAME = "aws_conn"
AWS_ANONYMOUS_CONN_NAME = "aws_anonymous_conn"

SOURCES_YAML_PATH = "./include/ingestion/sources.yaml"

ALERT_EMAILS = ["timchukilia@gmail.com"]

INGEST_FROM_S3_SOURCE_SCHEMES = ["s3"]