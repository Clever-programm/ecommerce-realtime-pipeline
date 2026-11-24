# Образ модулей конвейера (генераторы, модули извлечения).
FROM docker.m.daocloud.io/library/python:3.12-slim

WORKDIR /opt/pipeline

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/pipeline
