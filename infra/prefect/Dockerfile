FROM python:3.12.12-slim

ARG DBT_CORE_VERSION=1.11.5
ARG DBT_CH_VERSION=1.9.8

RUN apt-get update \
 && apt-get install -y --no-install-recommends git ca-certificates \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip \
 && pip install "prefect==3.6.24" "dbt-core==${DBT_CORE_VERSION}" "dbt-clickhouse==${DBT_CH_VERSION}" requests==2.32.3 PyYAML==6.0.2

CMD ["tail", "-f", "/dev/null"]
