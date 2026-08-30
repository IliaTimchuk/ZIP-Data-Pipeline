# How to define sources?

The `sources.yaml` file is the primary interface for configuring data sources used by the pipeline's DAGs. Each top-level key represents a source name, and its value is a mapping of configuration fields that describe where the source's data lives, how to authenticate, which DAGs consume it, and how to dynamically build the data paths.

## Configuration Template

```yaml
source_name:
  enabled: true
  base_url: "https://base_url"
  airflow_conn: "my_airflow_connection_name"
  dag_ids:
    - "first_dag_id"
    - "second_dag_id"
  endpoints:
    - path: "{base}/path/to/the/data/{frequency}/{date}"
      dataset_name: "{base}_{frequency}"
  keywords:
    base:
      - "root_1"
      - "root_2"
    frequency:
      - "hourly"
      - "daily"
```

## Field Definitions

| **Field** | **Type** | **Required?** | **Description** |
| --- | --- | --- | --- |
| **`enabled`** | `bool` | Optional (default: `true`) | If `false`, the source is skipped during load time for any requested DAG and will not go through validation. |
| **`base_url`** | `string` | Required | The root URL/URI. This is prepended to every resolved endpoint to build the final path. Must contain a scheme (e.g., `s3://`, `http://`, `https://`). Each DAG restricts which URL schemes it accepts (e.g. an S3-ingestion DAG may only allow `s3://`). If a source's `base_url` scheme isn't in the calling DAG's allowed list, the source fails structural validation — even if the URL is otherwise well-formed. |
| **`airflow_conn`** | `string` | Optional | Name of an Airflow Connection used to resolve credentials (e.g., AWS keys, basic auth). If omitted, the source is assumed to require no authentication. |
| **`dag_ids`** | `list[str]` | Optional | The DAGs permitted to use this source. If omitted or empty, the source is never attached to any DAG. A single source can feed multiple DAGs independently.  |
| **`endpoints`** | `list[str]` | Required | 	A list of endpoint definitions. Each entry is a dict with a path template (appended to base_url) and a dataset_name template used to label the resolved dataset. Both path and dataset_name may contain `{keyword}` segments. An endpoint missing either key fails validation.|
| **`keywords`** | `dict` | Optional | A mapping of string lists used to populate `{keyword}` segments in the endpoints. |

## Endpoint and Keyword Resolution

Endpoints are dynamically constructed using a combination of **keywords** (defined in the YAML) and **runtime parameters** (supplied by the DAG).

If a source defines `keywords`, all lists are combined via a Cartesian product. For example, 2 `base` values × 2 `frequency` values = 4 combinations per endpoint definition, applied to both its `path` and `dataset_name`.

Any `{keyword}` referenced in an endpoint's `path` or `dataset_name` but not defined in `keywords` is assumed to be a runtime parameter (e.g., `{date}`).  This is useful for dynamically changing endpoints (e.g., an S3 bucket endpoint that includes a date representing the period covered by this data).


### Complete `sources.yaml` Example


```yaml
finance:
  enabled: true
  base_url: "s3://data.finance"
  airflow_conn: "binance"
  dag_ids:
    - "dag_example"
  endpoints:
    - path: "data/monthly/trades/{symbol}/{symbol}-trades-{date}.zip"
      dataset_name: "{symbol}_trades"
  keywords:
    symbol:
      - "BTCUSDT"
      - "ETHUSDT"

weather_api:
  enabled: true
  base_url: "https://api.weather.example"
  dag_ids:
    - "datasets_to_s3"
    - "another_dag"
  endpoints:
    - path: "v1/reports/{date}.json"
      dataset_name: "weather_reports"
```

**What happens here:**

- `finance` generates 2 fully resolved endpoints for the `dag_example` DAG, injecting BTCUSDT and ETHUSDT for `{symbol}` in both `path` and `dataset_name`, and using the DAG's runtime `{date}`.
- `weather_api` does not use keywords or Airflow connections. It relies purely on the runtime `{date}` parameter. It will independently format and load for both `datasets_to_s3` and `another_dag` when they run.


<div style="height: 64px;"></div>


# How to read sources?

The `get_dag_sources(…)` function is the main interface for reading sources from `sources.yaml`. It reads the file, filters sources to those belonging to `dag_id`, and formats endpoints using keywords and `runtime_params`. It also validates that each source's `base_url` has the form `scheme://value`, where `scheme` is one of `allowed_schemes`.

```python
def get_dag_sources(
    dag_id: str, file_path: str, allowed_schemes: list[str], **runtime_params
) -> tuple[dict[str, dict], dict[str, list]]:
```

| Parameter | Type | Description |
| --- | --- | --- |
| `dag_id` | `str` | The DAG ID to filter sources by. Only sources whose `dag_ids` list includes this value are considered. |
| `file_path` | `str` | Path to `sources.yaml`. |
| `allowed_schemes` | `list[str]` | URL schemes (without `://`) this DAG accepts — e.g. `["s3"]`. Sources whose `base_url` scheme isn't in this list fail structural validation. **Set per-DAG by the caller**, not defined in `sources.yaml`. |
| `**runtime_params` | — | Arbitrary keyword arguments used to resolve `{placeholder}` segments in endpoints that aren't covered by `keywords` (e.g. `date=ds`). |

<div style="height: 32px;"></div>

| Return value | Type | Description |
| --- | --- | --- |
| `selected` | `dict[str, dict]` | Valid sources for `dag_id`, keyed by source name, with `endpoints` fully resolved (each a `{path, dataset_name}` dict). |
| `errors` | `dict[str, list]` | Source names mapped to their list of validation error messages. Empty if no errors occurred. |

## Loading Model

Source resolution is **DAG-scoped** and performed in a **single pass** using the `get_dag_sources(dag_id, file_path, allowed_schemes, **runtime_params)` function.

1. The YAML file is parsed, and sources are immediately filtered down to those whose `dag_ids` contains the requested `dag_id`. Sources belonging exclusively to other DAGs are skipped before any validation occurs.
2. Disabled sources (`enabled: false`) matching the `dag_id` are skipped.
3. For the filtered sources, all required fields are validated — including that `base_url` has a valid scheme with a non-empty value after it, and that every entry in `endpoints` is a dict containing both `path` and `dataset_name`. Malformed sources are skipped and reported, but they do not prevent other valid sources in the same DAG from being processed. The function also verifies that the DAG supports the scheme used by each source (list of allowed schemes for each DAG should be provided). It returns all validated sources along with the list of errors from the sources that failed validation.

<div style="height: 64px;"></div>

# How to define and validate dataset schemas?

The `dataset_schemas.yaml` file holds the expected column schema for each dataset produced by a source. Each top-level key is a `dataset_name` (matching the `dataset_name` resolved from a source's `endpoints` in `sources.yaml`), and its value is a mapping with a single `schema` field listing the expected column names, in order. It must not contain metadata columns, that are added in bronze. This should contain only the columns present in the raw source data - metadata columns added later during the bronze transformation  must not be included here.

```yaml
daily_trades:
  schema: ["id", "price", "qty", "quote_qty", "time", "is_buyer_maker"]
```

`read_dataset_schema_from_yaml(file_path, dataset_name)` reads this file and returns the `schema` list for `dataset_name`, raising a `KeyError` if no entry exists for that dataset.

Once the actual schema of an ingested file is known, `resolve_prefix_by_schema(expected_schema, actual_schema)` compares it against the expected schema from `dataset_schemas.yaml`. If the two match exactly (including column order), it returns `conf.VERIFIED_PREFIX`; otherwise it logs a warning with both schemas and returns `conf.UNVERIFIED_PREFIX`. This prefix is typically used to route the file into a verified or unverified location in storage.
