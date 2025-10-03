# Start from official Airflow image
FROM apache/airflow:2.9.3

# Install extra Python packages you need
RUN pip install --no-cache-dir \
    h2o \
    mlflow \
    scikit-learn \
    pandas \
    numpy \
    dbt-core \
    dbt-postgres
