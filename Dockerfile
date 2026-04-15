FROM python:3.12-slim

WORKDIR /project

RUN apt-get update && apt-get install -y --no-install-recommends git wget unzip && rm -rf /var/lib/apt/lists/*

# Install duckdb CLI binary
RUN wget -q https://github.com/duckdb/duckdb/releases/download/v1.5.2/duckdb_cli-linux-amd64.zip -O /tmp/duckdb.zip && \
    unzip -q /tmp/duckdb.zip -d /usr/local/bin && \
    rm /tmp/duckdb.zip && \
    chmod +x /usr/local/bin/duckdb
    
COPY dbt_octopus/package-lock.yml dbt_octopus/packages.yml ./dbt_octopus/

RUN python -m pip install --no-cache-dir dbt-duckdb duckdb pandas seaborn matplotlib

COPY . .

WORKDIR /project/dbt_octopus

RUN dbt deps

CMD ["sh", "-c", "dbt build && python exports.py && python visualize.py"]
