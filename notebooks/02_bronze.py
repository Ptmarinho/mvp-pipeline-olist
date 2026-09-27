# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Camada Bronze: ingestão dos CSVs
# MAGIC
# MAGIC **Extract + Load** dos 9 arquivos do Volume `landing` para tabelas Delta em `olist.bronze`.
# MAGIC
# MAGIC Princípio da camada: **o dado como ele veio**. Por isso:
# MAGIC - Todas as colunas são lidas como `STRING` (sem `inferSchema`), preservando o conteúdo original
# MAGIC   (ex.: CEPs com zero à esquerda, datas em texto). A tipagem acontece na Silver.
# MAGIC - Nenhuma linha é filtrada, deduplicada ou corrigida.
# MAGIC - São adicionados apenas metadados de controle: `_ingestion_ts` (momento da carga) e
# MAGIC   `_source_file` (arquivo de origem no Volume).
# MAGIC
# MAGIC Opções de leitura e por quê:
# MAGIC | Opção | Motivo |
# MAGIC |---|---|
# MAGIC | `header=true` | A primeira linha de cada CSV contém os nomes das colunas |
# MAGIC | `multiLine=true` + `escape='"'` | `order_reviews` possui comentários com quebra de linha dentro de aspas; sem isso, um registro vira várias linhas quebradas |
# MAGIC | `encoding=UTF-8` | Textos em português com acentos |
# MAGIC
# MAGIC Única intervenção técnica: `product_category_name_translation.csv` tem um caractere BOM (`﻿`)
# MAGIC no início do cabeçalho. Ele é removido **apenas do nome da coluna** para que o nome seja utilizável.
# MAGIC
# MAGIC Estratégia de carga: `overwrite` (carga completa e idempotente; reexecutar produz o mesmo resultado).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F


def ingerir_csv(tabela: str, arquivo: str) -> int:
    caminho = f"{VOLUME_PATH}/{arquivo}"
    df = (
        spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .csv(caminho)
        .select("*", F.col("_metadata.file_path").alias("_source_file"))
    )
    # Remove o BOM apenas do nome das colunas (conteúdo permanece intacto)
    df = df.toDF(*[c.replace("﻿", "").strip() for c in df.columns])
    df = df.withColumn("_ingestion_ts", F.current_timestamp())

    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{BRONZE}.{tabela}"))
    return spark.table(f"{BRONZE}.{tabela}").count()

# COMMAND ----------

resultado = []
for tabela, arquivo in ARQUIVOS_FONTE.items():
    qtd = ingerir_csv(tabela, arquivo)
    esperado = REGISTROS_ESPERADOS[tabela]
    resultado.append((f"{BRONZE}.{tabela}", arquivo, qtd, esperado, "OK" if qtd == esperado else "DIVERGENTE"))
    print(f"{tabela:<22} {qtd:>10,} linhas")

df_resultado = spark.createDataFrame(
    resultado, "tabela STRING, arquivo_origem STRING, linhas_carregadas LONG, linhas_esperadas LONG, status STRING"
)
display(df_resultado)

# COMMAND ----------

# Garantia: nenhuma perda/duplicação de registros na ingestão
divergentes = [r for r in resultado if r[4] != "OK"]
assert not divergentes, f"Contagem divergente na ingestão: {divergentes}"
print("Ingestão Bronze concluída: todas as contagens conferem com os arquivos de origem.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Amostra do dado bruto (tudo STRING, com metadados de ingestão)
# MAGIC SELECT * FROM bronze.orders LIMIT 10
