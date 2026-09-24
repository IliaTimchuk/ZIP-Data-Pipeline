# CONFIGURATIONS

# GENERAL CONFIGURATIONS
# Immutable raw zone containing original compressed (.zip) source archives.
LANDING_BUCKET = "landing"

# Raw extracted layer storing PyArrow-decompressed Parquet files.
BRONZE_BUCKET = "bronze"

# Cleansed operational layer storing deduplicated, type-casted Apache Iceberg tables.
SILVER_BUCKET = "silver"

# The emails that will be used to send notifications from Airflow DAGs on failure, retries, and etc.
ALERT_EMAILS = ["timchukilia@gmail.com"]


# LANDING CONFIGURATIONS
# The connection to the S3 (and S3-compatable) system.
AWS_CONN_NAME = "aws_conn"

# The connection that will be used to reach public buckets.
AWS_ANONYMOUS_CONN_NAME = "aws_anonymous_conn"

# The path to the source source.yaml and dataset_schemas.yaml files.
SOURCES_YAML_PATH = "./settings/sources.yaml"


# BRONZE CONFIGURATIONS
# The DAG that will be triggered in the ingestion_from_s3 DAG once the data arrived to the landing bucket.
BRONZE_DAG_ID = "bronze_transformation_zip_to_parquet"

# The bronze entypoint module.
BRONZE_ENTRYPOINT_MODULE = "src.bronze.entrypoint"

# The max size in bytes for the each decompressed JSON, -1 unlimited.
MAX_JSON_SIZE_BYTES = 8 * 1024 * 1024

# The Parquet layout of the bronze files, tuned for the Spark silver layer:
# files of roughly 128-512 MB, row groups of roughly 64-128 MB so that Spark can
# split large files on row-group boundaries. The row counts depend on the average
# row width.
PARQUET_MAX_ROWS_PER_FILE = 5_000_000
PARQUET_MAX_ROWS_PER_GROUP = 1_000_000
PARQUET_MIN_ROWS_PER_GROUP = 500_000


# CONSTANTS
# !!! NOT RECOMMENDED TO CHANGE !!!

# These templates are used to build a key for each layer.
LANDING_KEY_TEMPLATE = "{source_name}/{dataset_name}/ingest_date={date}/{file_name}"

# The bronze key of a ZIP holds all of its output and the _SUCCESS marker. It is
# wiped before each run.
BRONZE_KEY_TEMPLATE = (
    "{source_name}/{dataset_name}/ingest_date={date}/zip_name={zip_stem}"
)
BRONZE_VALIDATION_STATUS_KEY_TEMPLATE = (
    "{bronze_key}/schema_status={validation_status}"
)

# Written to the bronze key of a ZIP once its transformation is fully completed.
BRONZE_SUCCESS_MARKER = "_SUCCESS"

# Validation prefixes used in Bronze bucket keys. Resolved by schema validation.
VERIFIED_PREFIX = "verified"
UNVERIFIED_PREFIX = "unverified"
