import yaml
import itertools
import logging
from typing import Iterator

logger = logging.getLogger(__name__)


def _validate_source(
    source_name: str, source_data: dict, allowed_schemes: list[str]
) -> list[str]:
    """
    Returns a list of error messages for a source (empty if valid).

    Checks that base_url is present and has the form 'scheme://value'
    with scheme in allowed_schemes. Also, checks that at least one
    endpoint is defined.
    """
    errors = []

    base_url = source_data.get("base_url")

    if base_url is None:
        errors.append(f"Source '{source_name}' must have a 'base_url'.")
    else:
        scheme, sep, rest = base_url.partition("://")

        if not sep or scheme not in allowed_schemes:
            errors.append(
                f"Source '{source_name}' has base_url '{base_url}'; "
                f"expected one of {allowed_schemes} as the scheme."
            )
        elif not rest.rstrip("/"):
            errors.append(
                f"Source '{source_name}' has nothing after the scheme in base_url '{base_url}'."
            )

    endpoints = source_data.get("endpoints")
    if not endpoints:
        errors.append(
            f"At least one endpoint is required: the {source_name} source has no endpoints."
        )
    else:
        for endpoint in endpoints:
            if (
                not isinstance(endpoint, dict)
                or "path" not in endpoint
                or "dataset_name" not in endpoint
            ):
                errors.append(
                    f"Source '{source_name}' has an endpoint that is missing "
                    f"'path' or 'dataset_name': {endpoint}"
                )

    return errors


def _generate_keyword_combinations(keywords: dict[str, list]) -> Iterator[dict]:
    """Generates all parameter dictionaries from the Cartesian product of keywords."""
    for comb in itertools.product(*keywords.values()):
        yield dict(zip(keywords.keys(), comb))


def _format_endpoints(
    source_name: str, source_data: dict, runtime_params: dict
) -> tuple[list[dict], str | None]:
    """
    Formats endpoints (path, dataset_name) with keywords merges and
    runtime_params. Duplicate formatted endpoints are dropped.

    Stops and returns early on the first formatting error (e.g. an
    endpoint referencing an undefined parameter).

    Returns:
        (formatted_endpoints, error) — error is None on success, or a
        message string if any endpoint failed to format. On failure,
        formatted_endpoints contains only the endpoints formatted before
        the error occurred.
    """
    keywords = source_data.get("keywords") or {}
    keywords_combos = _generate_keyword_combinations(keywords)

    seen = set()
    formatted_endpoints = []

    for format_words_combo in keywords_combos:
        full_params = {**format_words_combo, **runtime_params}
        for endpoint in source_data["endpoints"]:

            path = endpoint["path"]
            dataset_name = endpoint["dataset_name"]

            try:
                formatted_path = path.format(**full_params)
                formatted_dataset_name = dataset_name.format(**full_params)
            except (KeyError, IndexError, ValueError) as e:
                error = (
                    f"Endpoint '{path}' or dataset name '{dataset_name}' in source "
                    f"'{source_name}' is invalid or references an undefined parameter: {e}"
                )
                return formatted_endpoints, error

            key = (formatted_path, formatted_dataset_name)
            if key not in seen:
                seen.add(key)
                formatted_endpoints.append(
                    {"path": formatted_path, "dataset_name": formatted_dataset_name}
                )

    return formatted_endpoints, None


def get_dag_sources(
    dag_id: str, file_path: str, allowed_schemes: list[str], **runtime_params
) -> tuple[dict[str, dict], dict[str, list]]:
    """
    Reads sources.yaml, filters to sources belonging to dag_id, and formats
    their endpoints using keywords and runtime_params. Validates that each
    source's base_url has the form 'scheme://value', where scheme is one of
    allowed_schemes.

    Sources that fail validation are skipped and reported in the returned
    errors dict, so a single malformed source does not prevent valid sources
    from being returned.

    Parameters:
        dag_id: The DAG ID to filter sources by.
        file_path: The path to the sources YAML file.
        allowed_schemes: Scheme prefixes (without '://') permitted for this DAG.
        **runtime_params: Runtime parameters to use for formatting endpoints.

    Returns:
        A tuple of (selected, errors):
            - selected: dict of valid sources belonging to dag_id, with
                endpoints (path, dataset_name) formatted using the provided
                runtime parameters.
            - errors: dict mapping source_name to a list of error message
                strings for that source.
    """
    with open(file_path, "r") as f:
        sources = yaml.safe_load(f) or {}

    selected = {}
    errors = {}

    for source_name, source_data in sources.items():

        dag_ids = source_data.get("dag_ids") or []
        if dag_id not in dag_ids:
            continue

        if not source_data.get("enabled", True):
            logger.info(
                "Source '%s' is disabled for DAG '%s' — skipping.", source_name, dag_id
            )
            continue

        if source_data.get("airflow_conn") is None:
            logger.info(
                "No airflow_conn set for %s — assuming this source needs no auth.",
                source_name,
            )

        source_errors = _validate_source(source_name, source_data, allowed_schemes)

        formatted_endpoints = []
        if not source_errors:
            formatted_endpoints, format_error = _format_endpoints(
                source_name, source_data, runtime_params
            )
            if format_error:
                source_errors.append(format_error)

        if source_errors:
            for msg in source_errors:
                logger.error(msg)
            errors[source_name] = source_errors
            continue

        selected[source_name] = {**source_data, "endpoints": formatted_endpoints}

    if not selected:
        logger.warning("The dag %s has no sources.", dag_id)

    return selected, errors


def prepare_s3_sources(sources: dict[str, dict], default_conn: str) -> list[dict]:
    """
    Prepare S3 sources to expand a task in Airflow DAG.

    Each source's base_url is stripped of its scheme prefix to get a bucket
    name, and each of its endpoints becomes its own dict:
    {source_name, dataset_name, bucket, key, aws_conn_id} — so a source with
    N endpoints yields N entries. Sources without an airflow_conn fall back to
    default_conn.

    Args:
        sources: A dict of sources, as returned by get_dag_sources, keyed by
            source name. Each source dict must contain:
                - base_url: The source location, as 'scheme://bucket-name'
                    (e.g. 's3://my-bucket').
                - endpoints: A list of dicts (path, dataset_name) to files in
                    the bucket.
                - airflow_conn (optional): The Airflow connection ID for the
                    source bucket.
        default_conn: The Airflow connection ID to use for sources that don't
            specify their own airflow_conn.

    Returns:
        A list of dicts, one per (source, endpoint) pair, each with the
        following keys:
            - source_name: The source name from the configuration file.
            - dataset_name: The dataset name from the configuration file that
                    is used to build a key in the landing bucket. 
            - bucket: The source bucket name.
            - key: The source key (path) to the file in the bucket.
            - aws_conn_id: The Airflow connection ID to use when reading
                the file — either the source's own airflow_conn, or
                default_conn if none was set.
    """
    kwargs_to_expand = []

    for source_name, source_data in sources.items():
        conn = source_data.get("airflow_conn", default_conn)
        bucket = source_data["base_url"].split("://", 1)[-1].rstrip("/")

        for endpoint in source_data["endpoints"]:
            kwargs_to_expand.append(
                {
                    "source_name": source_name,
                    "dataset_name": endpoint["dataset_name"],
                    "bucket": bucket,
                    "key": endpoint["path"],
                    "aws_conn_id": conn,
                }
            )

    return kwargs_to_expand


def get_error_message(errors: dict[str, list]) -> str:
    """Returns a formatted message to notify about errors."""
    error_items = "".join(
        f"<li><b>{name}</b>: {'; '.join(msgs)}</li>" for name, msgs in errors.items()
    )
    return f"Malformed sources:<ul>{error_items}</ul>"