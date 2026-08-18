import logging
import pytest
import yaml

from include.ingestion.read_sources import (
    _validate_source,
    _format_endpoints,
    get_dag_sources,
    prepare_s3_sources,
    get_error_message,
)

ALLOWED_SCHEMES = ["s3", "https"]


@pytest.fixture
def create_source_yaml(tmp_path):
    def _create(data: dict) -> str:
        path = tmp_path / "sources.yaml"
        with open(path, "w") as f:
            yaml.safe_dump(data, f)
        return str(path)

    return _create


# _validate_source


@pytest.mark.parametrize(
    "source_data, expected_errors",
    [
        pytest.param(
            {"base_url": "s3://my-bucket", "endpoints": ["path/to/file"]},
            [],
            id="valid",
        ),
        pytest.param(
            {"base_url": "ftp://my-bucket", "endpoints": ["path/to/file"]},
            [
                "Source 'my_source' has base_url 'ftp://my-bucket'; "
                f"expected one of {ALLOWED_SCHEMES} as the scheme."
            ],
            id="invalid_scheme",
        ),
        pytest.param(
            {"base_url": "www.example.com/no-scheme-separator", "endpoints": ["path"]},
            [
                "Source 'my_source' has base_url 'www.example.com/no-scheme-separator'; "
                f"expected one of {ALLOWED_SCHEMES} as the scheme."
            ],
            id="missing_scheme_separator",
        ),
        pytest.param(
            {},
            [
                "Source 'my_source' must have a 'base_url'.",
                "At least one endpoint is required: the my_source source has no endpoints.",
            ],
            id="missing_everything",
        ),
    ],
)
def test_validate_source(source_data, expected_errors) -> None:
    assert (
        _validate_source("my_source", source_data, ALLOWED_SCHEMES) == expected_errors
    )


# _format_endpoints


def test_format_endpoints_static_and_runtime_params() -> None:
    source_data = {"endpoints": ["static/path", "data/{date}"]}
    formatted, error = _format_endpoints(
        "my_source", source_data, {"date": "2026-01-01"}
    )
    assert error is None
    assert formatted == ["static/path", "data/2026-01-01"]


def test_format_endpoints_with_keywords() -> None:
    source_data = {
        "endpoints": ["{base}/{frequency}"],
        "keywords": {
            "base": ["root_1", "root_2"],
            "frequency": ["daily"],
        },
    }
    formatted, error = _format_endpoints("my_source", source_data, {})
    assert error is None
    assert formatted == ["root_1/daily", "root_2/daily"]


def test_format_undefined_placeholder_returns_error() -> None:
    source_data = {"endpoints": ["data/{undefined_param}"]}
    formatted, error = _format_endpoints("my_source", source_data, {})
    assert formatted == []
    assert error == (
        "Endpoint 'data/{undefined_param}' in source 'my_source' "
        "is invalid or references an undefined parameter: 'undefined_param'"
    )


# get_dag_sources


def test_get_dag_sources_filters_and_formats_correctly(create_source_yaml) -> None:
    yaml_data = {
        "my_source": {
            "base_url": "s3://my-bucket",
            "endpoints": ["data/{date}"],
            "dag_ids": ["dag_1"],
        },
        "other_source": {
            "base_url": "s3://other-bucket",
            "endpoints": ["path"],
            "dag_ids": ["dag_2"],
        },
    }
    yaml_path = create_source_yaml(yaml_data)
    selected, errors = get_dag_sources(
        "dag_1", yaml_path, ALLOWED_SCHEMES, date="2026-08-02"
    )

    assert list(selected.keys()) == ["my_source"]
    assert selected["my_source"]["endpoints"] == ["data/2026-08-02"]
    assert errors == {}


def test_get_dag_sources_handles_disabled_and_invalid_sources(
    create_source_yaml,
) -> None:
    yaml_data = {
        "disabled_source": {
            "base_url": "s3://my-bucket",
            "endpoints": ["path"],
            "dag_ids": ["dag_1"],
            "enabled": False,
        },
        "invalid_source": {
            "base_url": "ftp://my-bucket",
            "endpoints": ["path"],
            "dag_ids": ["dag_1"],
        },
    }
    yaml_path = create_source_yaml(yaml_data)
    selected, errors = get_dag_sources("dag_1", yaml_path, ALLOWED_SCHEMES)

    assert selected == {}
    assert "disabled_source" not in errors
    assert errors == {
        "invalid_source": [
            "Source 'invalid_source' has base_url 'ftp://my-bucket'; "
            f"expected one of {ALLOWED_SCHEMES} as the scheme."
        ]
    }


def test_get_dag_sources_omitted_or_empty_dag_ids_skipped(create_source_yaml) -> None:
    yaml_data = {
        "no_dag_ids_key": {"base_url": "s3://my-bucket", "endpoints": ["path"]},
        "empty_dag_ids": {
            "base_url": "s3://my-bucket",
            "endpoints": ["path"],
            "dag_ids": [],
        },
    }
    yaml_path = create_source_yaml(yaml_data)
    selected, errors = get_dag_sources("dag_1", yaml_path, ALLOWED_SCHEMES)

    assert selected == {}
    assert errors == {}


def test_get_dag_sources_logs_errors_and_warnings(caplog, create_source_yaml) -> None:
    yaml_data = {
        "malformed_source": {
            "base_url": "ftp://my-bucket",
            "endpoints": ["path"],
            "dag_ids": ["dag_1"],
        }
    }
    yaml_path = create_source_yaml(yaml_data)

    with caplog.at_level(logging.INFO):
        selected, errors = get_dag_sources("dag_1", yaml_path, ALLOWED_SCHEMES)

    assert selected == {}

    error_logs = [r.message for r in caplog.records if r.levelname == "ERROR"]
    warning_logs = [r.message for r in caplog.records if r.levelname == "WARNING"]

    assert error_logs == [
        "Source 'malformed_source' has base_url 'ftp://my-bucket'; "
        f"expected one of {ALLOWED_SCHEMES} as the scheme."
    ]
    assert warning_logs == ["The dag dag_1 has no sources."]


# prepare_s3_sources


def test_get_mapped_s3_sources_maps_and_assigns_connections() -> None:
    sources = {
        "my_source": {
            "base_url": "s3://my-bucket/subfolder",
            "endpoints": ["a.csv", "b.csv"],
            "airflow_conn": "my_conn",
        },
        "other_source": {
            "base_url": "s3://other-bucket",
            "endpoints": ["c.csv"],
            # missing conn should fall back to default
        },
    }
    result = prepare_s3_sources(sources, "default_conn")

    assert result == [
        {"bucket": "my-bucket/subfolder", "key": "a.csv", "aws_conn_id": "my_conn"},
        {"bucket": "my-bucket/subfolder", "key": "b.csv", "aws_conn_id": "my_conn"},
        {"bucket": "other-bucket", "key": "c.csv", "aws_conn_id": "default_conn"},
    ]


# get_error_message


def test_get_error_message_formats_multiple_sources_and_errors() -> None:
    errors = {
        "my_source": ["error one", "error two"],
        "other_source": ["error three"],
    }
    message = get_error_message(errors)

    assert message == (
        "Malformed sources:<ul>"
        "<li><b>my_source</b>: error one; error two</li>"
        "<li><b>other_source</b>: error three</li>"
        "</ul>"
    )
