# Образ потоковой обработки: Spark 3.5 + драйверы приёмников.
FROM docker.m.daocloud.io/apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root
ARG PIP_INDEX_URL=https://pypi.org/simple
RUN pip install --no-cache-dir -i ${PIP_INDEX_URL} \
        clickhouse-connect==0.8.11 \
        minio==7.2.9 \
        psycopg2-binary==2.9.10 \
        pandas==2.2.3 \
        pyarrow==18.1.0 \
        PyYAML==6.0.2

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/pipeline
