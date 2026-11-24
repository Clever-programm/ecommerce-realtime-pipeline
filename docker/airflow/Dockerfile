# Образ оркестратора: Apache Airflow + зависимости задач конвейера.
FROM docker.m.daocloud.io/apache/airflow:2.10.4-python3.12

USER root
# postgresql-client требуется задаче резервного копирования хранилища витрин.
RUN apt-get update \
 && apt-get install -y --no-install-recommends postgresql-client \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
ARG PIP_INDEX_URL=https://pypi.org/simple
RUN pip install --no-cache-dir -i ${PIP_INDEX_URL} \
        requests==2.32.3 \
        PyYAML==6.0.2 \
        minio==7.2.9 \
        psycopg2-binary==2.9.10 \
        clickhouse-connect==0.8.11 \
        pandas==2.1.4 \
        pyarrow==18.1.0 \
        great-expectations==1.3.9

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/pipeline
