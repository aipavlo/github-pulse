FROM python:3.12.12-slim

ARG DBT_CORE_VERSION=1.11.5
ARG DBT_CH_VERSION=1.9.8

RUN apt-get update \
 && apt-get install -y --no-install-recommends git ca-certificates \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip \
 && pip install "prefect==3.6.24" "dbt-core==${DBT_CORE_VERSION}" "dbt-clickhouse==${DBT_CH_VERSION}"

COPY requirements.txt /tmp/requirements.txt

RUN pip install -r /tmp/requirements.txt

CMD ["tail", "-f", "/dev/null"]
