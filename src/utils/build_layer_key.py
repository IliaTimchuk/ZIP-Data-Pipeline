import posixpath
import settings.pipeline_config as conf


def build_landing_key(source_name: str, source_key: str, dataset_name: str, date: str):
    """
    Builds a bronze key based on the LANDING_KEY_TEMPLATE from
    settings/pipeline_config.py. Uses the last part of the key as
    the file name.
    """
    file_name = source_key.rsplit("/", 1)[-1]

    landing_key = conf.LANDING_KEY_TEMPLATE.format(
        source_name=source_name,
        dataset_name=dataset_name,
        date=date,
        file_name=file_name,
    )
    return landing_key


def build_bronze_key(landing_key: str) -> str:
    """
    Builds the bronze key of a ZIP from its landing key, based on the
    BRONZE_KEY_TEMPLATE from settings/pipeline_config.py:
        {source_name}/{dataset_name}/ingest_date={date}/zip_name={zip_stem}

    The key holds all the output of the ZIP and its _SUCCESS marker.
    """
    source_name, dataset_name, date_partition, zip_name = landing_key.split("/")
    return conf.BRONZE_KEY_TEMPLATE.format(
        source_name=source_name,
        dataset_name=dataset_name,
        date=date_partition.split("=", 1)[1],
        zip_stem=posixpath.splitext(zip_name)[0],
    )


def build_bronze_validation_status_key(
    landing_key: str, validation_status: str
) -> str:
    """
    Builds the key for the files of a ZIP with the given validation status,
    based on the BRONZE_VALIDATION_STATUS_KEY_TEMPLATE:
        {bronze_key}/schema_status={validation_status}
    """
    return conf.BRONZE_VALIDATION_STATUS_KEY_TEMPLATE.format(
        bronze_key=build_bronze_key(landing_key),
        validation_status=validation_status,
    )
