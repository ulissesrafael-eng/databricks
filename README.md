# Pipeline Databricks - IBGE para Delta Raw

Este repositório contém um pipeline em Python para Databricks que:

1. Consulta dados públicos do **IBGE SIDRA** via API REST.
2. Normaliza nomes de colunas para `snake_case`.
3. Adiciona metadados de ingestão (`ingestion_ts`, `source_system`, `source_url`).
4. Persiste os dados em tabela **Delta** na camada `raw`.

## Arquivos principais

- `pipelines/ibge_raw_pipeline.py`: pipeline de ingestão e carga Delta.
- `sql/create_raw_ibge_table.sql`: script opcional para pré-criação da tabela Delta.

## Como executar no Databricks

### Opção 1: Databricks Job (Python file)

1. Faça upload do arquivo `pipelines/ibge_raw_pipeline.py` para Workspace/Repos.
2. Crie um Job com tarefa Python apontando para esse arquivo.
3. Execute com um cluster que tenha acesso à internet.

### Opção 2: Notebook

Em uma célula Python do Databricks:

```python
from pipelines.ibge_raw_pipeline import run_pipeline

run_pipeline(
    sidra_path="t/1419/n1/all/v/63/p/last%2012",
    database="raw",
    table="ibge_sidra"
)
```

## Observações

- O endpoint padrão usa a tabela `1419` (IPCA) apenas como exemplo.
- Para trocar o dataset, ajuste o parâmetro `sidra_path` seguindo a documentação do SIDRA.
- A escrita está em `append` com `mergeSchema=true`, útil para evolução de esquema na camada raw.
