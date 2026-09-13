import pyarrow as pa

# Format:  {dataset_name: pyarrow.Schema}
bronze_schemas = {
    "daily_trades": pa.schema([
        ("id", pa.string()),
        ("price", pa.string()),
        ("qty", pa.string()),
        ("quote_qty", pa.string()),
        ("time", pa.string()),
        ("is_buyer_maker", pa.string())
    ])
}