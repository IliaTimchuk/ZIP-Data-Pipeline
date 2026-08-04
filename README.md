# **Data-Lakehouse**
This project aims to build a layered Data Lake architecture using an ELT pipeline.

![The Data Lakehouse's architecture](./docs/images/architecture-overview.png)


## **Tech Stack**

- **Orchestration:** [Apache Airflow](https://airflow.apache.org/).
- **Processing:** [Apache Spark](https://spark.apache.org/docs/latest/) - Large-scale data transformations, [PyArrow](https://arrow.apache.org/docs/python/index.html) - lightweight transformations.
- **Data Lake:** [MinIO](https://docs.min.io/aistor/) + [Apache Iceberg](https://iceberg.apache.org/).
- **Data Warehouse:** [ClickHouse](https://clickhouse.com/).
- **Infrastructure:** [Docker](https://www.docker.com/).
- **Tests & CI:** [Pytest](https://docs.pytest.org/en/stable/), [GitHub Actions](https://docs.github.com/en/actions).

## **Workflow**

### **Sources Configuration**
All data sources are defined declaratively in a single YAML file (`include/settings/sources.yaml`). This is the main interface for adding, disabling, or modifying a source. Each entry configures the data's location, authentication, target DAGs, and dynamic endpoint construction. A single source can feed multiple DAGs independently, and each source is validated individually to ensure that malformed configurations never break the rest of the pipeline.

Read more: 
- [How to define sources?](include/settings/README.md)
- [How to read sources?](include/ingestion/README.md)


### **Ingestion**
The ingestion layer is represented by a DAG (<code>[ingest_from_s3](dags/ingestion.py)</code>) that extracts data from the S3 bucket, which provides daily data in ZIP format, and saves it to the MinIO landing bucket. There are three main tasks in the ingestion DAG:

1. The first task (`load_sources_config`) loads sources by dag_id from the sources configuration YAML file. It passes the logical_date and dag_id from the context to the parser function (`get_dag_sources`) to retrieve endpoints and other source metadata. It then formats the output as required by the task group (extract_files) to expand the tasks.
   
2. The second task (`wait_for_file`) uses `S3KeySensor` to wait for the endpoint to be available.
   > *Note:* Currently, the `S3KeySensor` uses the "reschedule" mode. On a big scale, it makes sense to consider the "deferrable" mode. The airflow-triggerer is required for this mode, so it has to be defined.

3. The third task (`stream_file`) streams a file from a source S3 bucket into the Landing bucket in chunks using boto3. It also checks the uploaded object's size against the size on the source platform, deletes it if the size mismatches.
   > *Note:* This task can be replaced by `S3CopyObjectOperator` if Amazon S3 is used instead of MinIO. It operates on the provider side, so the Airflow Worker's memory will never be touched. Also, this operation can be performed in a separate container to protect the Airflow Worker and save time when the file is huge.

The second and third tasks are united into the task group (`extract_files`) to create a 1-1 mapping. One instance of this task group is created per source/endpoint combination, so each instance handles exactly one (bucket, key) pair.


### **Bronze (in-progress)**
PyArrow in a separate Docker container decompresses ZIP files in chunks, validates the schema, and writes the result as Parquet into the Bronze bucket. This approach was chosen because ZIP files are unsplittable, so Spark can't process them in parallel. The combination of Docker and PyArrow provides fast processing and horizontal scalability through the Airflow DockerOperator/KubernetesPodOperator (in case we had a real distributed cluster). The open-source <code>[stream-unzip](https://stream-unzip.docs.trade.gov.uk/)</code> function is used to provide efficient memory usage. It allows reading and processing ZIP files in chunks.

### **Silver (planned)**
Spark handles splittable Parquet files from the Bronze bucket. It cleans, transforms, and writes them as Apache Iceberg tables into the Silver bucket.

### **Gold (planned)**
Spark handles Apache Iceberg tables from the Silver bucket, builds data marts using denormalization, and loads the result to ClickHouse.

## **Project Structure**

```
.
├── dags/
│   └── ingestion.py              # Airflow DAG: waits for source files in S3, streams them to landing bucket
├── docker/                       # Custom Dockerfiles and requirements for Airflow, Spark, PyArrow images
├── include/
│   ├── bronze/
│   │   └── preprocess_zip.py     # Landing -> Bronze transformation
│   ├── ingestion/
│   │   ├── extract_datasets.py   # S3 upload streaming and file size validation helpers
│   │   └── read_sources.py       # sources.yaml readers
│   ├── settings/ 
│   │   ├── sources.yaml          # Interface to define sources
│   │   └── pipeline_config.py    # Pipeline cofigurations              
│   └── utils/                    # Shared helpers
├── tests/
│   ├── dag_tests/
│   └── unit_tests/
├── .gitignore
├── docker-compose.yaml           # Infrastructure setup (Airflow, Spark, MinIO)
└── requirements.txt              # Python dependencies
```

## Docker Services 
| Service | URL | Login |
| --- | --- | --- |
| Airflow UI | `http://localhost:8080` | `AIRFLOW_USERNAME` / `AIRFLOW_PASSWORD` |
| MinIO Console | `http://localhost:9002` | `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` |
| Spark Master UI | `http://localhost:8090` | — |
| Spark History Server | `http://localhost:18080` | — |


### Visit the [documentation](https://app.notion.com/p/The-Data-Lakehouse-Documentation-3846b6177dd7808a9514ee48de11ceff?source=copy_link) on Notion for more details.