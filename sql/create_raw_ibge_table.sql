-- Script opcional para provisionar estrutura da camada raw no Databricks SQL
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.ibge_sidra (
  d1c STRING,
  d1n STRING,
  d2c STRING,
  d2n STRING,
  d3c STRING,
  d3n STRING,
  d4c STRING,
  d4n STRING,
  n STRING,
  v STRING,
  ingestion_ts TIMESTAMP,
  source_system STRING,
  source_url STRING
)
USING DELTA;
