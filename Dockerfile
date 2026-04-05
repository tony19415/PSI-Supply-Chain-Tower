FROM apache/airflow:2.10.0-python3.11

USER root
# Install system-level dependencies for DuckDB and Python compilation
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# Copy requirements first to leverage Docker caching
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt