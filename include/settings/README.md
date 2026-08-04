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
    - "{base}/path/to/the/data/{frequency}/{date}"
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
| **`endpoints`** | `list[str]` | Required | A list of path templates appended to `base_url`. May contain `{placeholder}` segments.  |
| **`keywords`** | `dict` | Optional | A mapping of string lists used to populate `{placeholder}` segments in the endpoints. |

## Endpoint and Keyword Resolution

Endpoints are dynamically constructed using a combination of **keywords** (defined in the YAML) and **runtime parameters** (supplied by the DAG via Python kwargs).

If a source defines `keywords`, all lists are combined via a Cartesian product. For example, 2 `base` values × 2 `frequency` values = 4 combinations per endpoint template.

Any `{placeholder}` referenced in an endpoint but not defined in `keywords` is assumed to be a runtime parameter (e.g., `{date}`).  This is useful for dynamically changing endpoints (e.g., an S3 bucket endpoint that includes a date representing the period covered by this data).


### Complete `sources.yaml` Example


```yaml
finance:
  enabled: true
  base_url: "s3://data.binance.vision"
  airflow_conn: "binance"
  dag_ids:
    - "dag_example"
  endpoints:
    - "data/futures/um/monthly/trades/{symbol}/{symbol}-trades-{date}.zip"
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
    - "v1/reports/{date}.json"
```

**What happens here:**

- `finance` generates 2 fully resolved endpoints for the `dag_example` DAG, injecting `BTCUSDT` and `ETHUSDT` for `{symbol}`, and using the DAG's runtime `{date}`.
- `weather_api` does not use keywords or Airflow connections. It relies purely on the runtime `{date}` parameter. It will independently format and load for both `datasets_to_s3` and `another_dag` when they run.

### Visit the [documentation](https://app.notion.com/p/The-Data-Lakehouse-Documentation-3846b6177dd7808a9514ee48de11ceff?source=copy_link) on Notion for more details.