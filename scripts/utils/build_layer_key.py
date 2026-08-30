import posixpath
import settings.pipeline_config as conf


def build_landing_key(source_name: str, source_key: str, dataset_name: str, date: str):
    """
    Builds a bronze key based on the LANDING_KEY_TEMPLATE from
    settings/pipeline_config.py.
    """
    file_name = source_key.rsplit("/", 1)[-1]

    landing_key = conf.LANDING_KEY_TEMPLATE.format(
        source_name=source_name,
        dataset_name=dataset_name,
        date=date,
        file_name=file_name,
    )
    return landing_key


def build_bronze_key(landing_key: str, validation_prefix: str = None) -> str:
    """
    Builds a bronze key based on the landing_key and BRONZE_KEY_TEMPLATE from
    settings/pipeline_config.py.
    """
    source_name, dataset_name, date, file_name = landing_key.split("/")
    bronze_key = conf.BRONZE_KEY_TEMPLATE.format(
        source_name=source_name,
        dataset_name=dataset_name,
        landing_date=date,
        validation_prefix=validation_prefix,
        file_stem=posixpath.splitext(file_name)[0],
    )
    return bronze_key
