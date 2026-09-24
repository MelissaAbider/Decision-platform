FROM apache/airflow:2.10.5-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV UV_PROJECT_ENVIRONMENT=/opt/airflow/.venvs/ia-decision-platform
ENV UV_LINK_MODE=copy
ENV PATH="/home/airflow/.local/bin:${PATH}"

RUN python -m pip install --no-cache-dir uv psycopg2-binary

WORKDIR /opt/airflow/project
COPY --chown=airflow:0 pyproject.toml uv.lock README.md ./
COPY --chown=airflow:0 src ./src

RUN uv sync --frozen --extra spark --extra quality --extra serving
