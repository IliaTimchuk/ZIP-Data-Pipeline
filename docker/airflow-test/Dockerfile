FROM apache/airflow:3.2.0
COPY requirements.airflow-test.txt /requirements.airflow-test.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.airflow-test.txt