FROM apache/airflow:2.1.0
USER airflow
RUN pip install --no-cache-dir --user beautifulsoup4