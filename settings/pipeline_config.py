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

# The path to the bronze entypoint.
BRONZE_ENTRYPOINT_MODULE = "src.bronze.entrypoint"

# The max size in bytes for the each decompressed JSON, -1 unlimited.
MAX_JSON_SIZE_BYTES = 8 * 1024 * 1024


# CONSTANTS
# !!! NOT RECOMMENDED TO CHANGE !!!

# These are used to build a key for each layer.
LANDING_KEY_TEMPLATE = "{source_name}/{dataset_name}/date={date}/{file_name}"
BRONZE_KEY_TEMPLATE = (
    "{source_name}/{dataset_name}/{landing_date}/status={verification_status}/{file_stem}"
)

# Validation prefixes used in Bronze bucket keys. Resolved by schema validation.
VERIFIED_PREFIX = "verified"
UNVERIFIED_PREFIX = "unverified"
