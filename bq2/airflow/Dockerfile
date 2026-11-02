# One slim Airflow 2 container for the BQ2 batch and monitor DAGs (decided 2 Nov).
FROM apache/airflow:2.11.2-python3.12
# Same versions as the laptop's uv environment, so the live model scores identically.
RUN pip install --no-cache-dir xgboost==3.4.1 pandas==3.0.6 pyarrow==26.0.0 numpy==2.5.4 boto3 pytest
