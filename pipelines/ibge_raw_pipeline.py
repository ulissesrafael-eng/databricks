"""Pipeline Databricks para ingestão de dados do IBGE (SIDRA) na camada raw.

Este script pode ser executado em um Job do Databricks ou em um notebook Python.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Iterable

import requests
from pyspark.sql import DataFrame, SparkSession, functions as F


# Endpoint de exemplo do SIDRA:
# - t/1419: tabela de IPCA por subitem
# - n1/all : Brasil (todos no nível Brasil)
# - v/63   : variável índice
# - p/last 12: últimos 12 períodos
DEFAULT_SIDRA_PATH = "t/1419/n1/all/v/63/p/last%2012"


def _normalize_column_name(name: str) -> str:
    """Converte nome de coluna para snake_case compatível com Delta."""
    value = name.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_")


def fetch_sidra_rows(sidra_path: str = DEFAULT_SIDRA_PATH) -> tuple[list[dict], str]:
    """Busca dados do SIDRA/IBGE e retorna registros e URL utilizada."""
    source_url = f"https://apisidra.ibge.gov.br/values/{sidra_path}?formato=json"
    response = requests.get(source_url, timeout=60)
    response.raise_for_status()

    payload = response.json()
    if not isinstance(payload, list) or len(payload) < 2:
        raise ValueError("Resposta do SIDRA sem dados para ingestão.")

    # O primeiro item costuma trazer o cabeçalho da API.
    records = payload[1:]
    if not records:
        raise ValueError("A consulta retornou lista vazia de registros.")

    return records, source_url


def build_raw_dataframe(spark: SparkSession, records: Iterable[dict], source_url: str) -> DataFrame:
    """Cria DataFrame Spark com metadados de ingestão para camada raw."""
    df = spark.createDataFrame(list(records))

    renamed_columns = [
        F.col(c).alias(_normalize_column_name(c))
        for c in df.columns
    ]
    normalized_df = df.select(*renamed_columns)

    ingested_at = datetime.now(timezone.utc).isoformat()

    return (
        normalized_df
        .withColumn("ingestion_ts", F.to_timestamp(F.lit(ingested_at)))
        .withColumn("source_system", F.lit("IBGE_SIDRA"))
        .withColumn("source_url", F.lit(source_url))
    )


def write_raw_delta(df: DataFrame, database: str = "raw", table: str = "ibge_sidra") -> None:
    """Escreve DataFrame como tabela Delta gerenciada na camada raw."""
    spark = df.sparkSession
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(f"{database}.{table}")
    )


def run_pipeline(
    sidra_path: str = DEFAULT_SIDRA_PATH,
    database: str = "raw",
    table: str = "ibge_sidra",
) -> None:
    """Executa pipeline completo: extração IBGE -> transformação -> Delta raw."""
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    records, source_url = fetch_sidra_rows(sidra_path=sidra_path)
    raw_df = build_raw_dataframe(spark=spark, records=records, source_url=source_url)
    write_raw_delta(df=raw_df, database=database, table=table)


if __name__ == "__main__":
    run_pipeline()
