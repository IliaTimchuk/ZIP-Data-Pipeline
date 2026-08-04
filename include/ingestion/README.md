# How to read sources?

The `get_dag_sources(…)` function is the main interface for reading sources from `sources.yaml`. It reads the file, filters sources to those belonging to `dag_id`, and formats endpoints using keywords and `runtime_params`. It also validates that each source’s `base_url` has the form `scheme://value`, where `scheme` is one of `allowed_schemes`.

```python
def get_dag_sources(
    dag_id: str, path: str, allowed_schemes: list[str], **runtime_params
) -> tuple[dict[str, dict], dict[str, list]]:
```

| Parameter | Type | Description |
| --- | --- | --- |
| `dag_id` | `str` | The DAG ID to filter sources by. Only sources whose `dag_ids` list includes this value are considered. |
| `path` | `str` | Path to `sources.yaml`. |
| `allowed_schemes` | `list[str]` | URL schemes (without `://`) this DAG accepts — e.g. `["s3"]`. Sources whose `base_url` scheme isn't in this list fail structural validation. **Set per-DAG by the caller**, not defined in `sources.yaml`. |
| `**runtime_params` | — | Arbitrary keyword arguments used to resolve `{placeholder}` segments in endpoints that aren't covered by `keywords` (e.g. `date=ds`). |

**Returns:** `(selected, errors)`

| Return value | Type | Description |
| --- | --- | --- |
| `selected` | `dict[str, dict]` | Valid sources for `dag_id`, keyed by source name, with `endpoints` fully resolved. |
| `errors` | `dict[str, list]` | Source names mapped to their list of validation error messages. Empty if no errors occurred. |

Each source goes through two validation stages before it's considered usable:

| Stage | Checks |
| --- | --- |
| **Structural** | `base_url` is present and matches `scheme://value`, with `scheme` in the DAG's `allowed_schemes`; at least one endpoint is defined |
| **Endpoint formatting** | Every endpoint template can be resolved via `.format()` using the source's `keywords` combined with the DAG's runtime parameters (e.g. `date`) |

Structural checks run first. If a source fails structural validation, endpoint formatting is skipped for that source entirely.

Each error is logged by `logger.error`. If no sources are selected at all, a warning is logged (`logger.warning`) at the DAG level.

## Loading Model

Source resolution is **DAG-scoped** and performed in a **single pass** using the `get_dag_sources(dag_id, path, allowed_schemes, **runtime_params)` function.

1. The YAML file is parsed, and sources are immediately filtered down to those whose `dag_ids` contains the requested `dag_id`. Sources belonging exclusively to other DAGs are skipped before any validation occurs.
2. Disabled sources (`enabled: false`) matching the `dag_id` are skipped.
3. For the filtered sources, all required fields are validated. Malformed sources are skipped and reported, but they do not prevent other valid sources in the same DAG from being processed. The function also verifies that the DAG supports the scheme used by each source. It returns all validated sources along with the list of errors from the sources that failed validation.

### Visit the [documentation](https://app.notion.com/p/The-Data-Lakehouse-Documentation-3846b6177dd7808a9514ee48de11ceff?source=copy_link) on Notion for more details.