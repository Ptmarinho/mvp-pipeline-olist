# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Setup do ambiente
# MAGIC
# MAGIC Cria os schemas da arquitetura medalhão e o Volume de pouso (landing) no Unity Catalog.
# MAGIC Pode ser executado várias vezes sem efeito colateral (`IF NOT EXISTS`).
# MAGIC
# MAGIC **Após executar:** faça o upload dos 9 CSVs do Kaggle no Volume
# MAGIC (*Catalog → olist → bronze → Volumes → landing → Upload to this volume*)
# MAGIC e rode a última célula para conferir se todos chegaram.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

schemas = {
    BRONZE: "Camada Bronze: dados brutos do dataset Olist exatamente como recebidos (colunas STRING) + metadados de ingestão.",
    SILVER: "Camada Silver: dados Olist limpos, tipados, deduplicados e padronizados.",
    GOLD: "Camada Gold: modelo dimensional (Esquema Estrela) pronto para análises de entrega, satisfação e vendas.",
    QUALIDADE: "Resultados das verificações de qualidade de dados (perfil da Bronze e validações da Gold).",
}

for schema, comentario in schemas.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema} COMMENT '{comentario}'")
    print(f"Schema OK: {schema}")

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {VOLUME}
    COMMENT 'Área de pouso dos CSVs originais do Kaggle (olistbr/brazilian-ecommerce), sem alteração.'
""")
print(f"Volume OK: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência dos arquivos no Volume

# COMMAND ----------

try:
    presentes = {f.name: f.size for f in dbutils.fs.ls(VOLUME_PATH)}
except Exception:
    presentes = {}

linhas = []
for tabela, arquivo in ARQUIVOS_FONTE.items():
    tamanho = presentes.get(arquivo)
    linhas.append((arquivo, tabela, "OK" if tamanho else "FALTANDO", round((tamanho or 0) / 1024 / 1024, 2)))

display(spark.createDataFrame(linhas, "arquivo STRING, tabela_bronze STRING, status STRING, tamanho_mb DOUBLE"))

faltando = [l[0] for l in linhas if l[2] != "OK"]
if faltando:
    print(f"Faça o upload destes arquivos em {VOLUME_PATH}: {faltando}")
else:
    print("Todos os 9 arquivos estão no Volume. Pode seguir para 02_bronze.")
