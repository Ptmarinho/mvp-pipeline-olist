# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Camada Silver: limpeza e padronização
# MAGIC
# MAGIC **Transform + Load** de `olist.bronze` para `olist.silver`. Cada tabela recebe:
# MAGIC 1. **Tipagem** correta (STRING → TIMESTAMP, DECIMAL, INT, DOUBLE);
# MAGIC 2. **Padronização** de textos (cidade sem acento e em minúsculas, UF em maiúsculas, CEP com 5 dígitos);
# MAGIC 3. **Deduplicação** e tratamento de nulos conforme os problemas medidos em `03_qualidade_bronze`;
# MAGIC 4. **Colunas derivadas** úteis para as análises (prazos de entrega, atraso);
# MAGIC 5. Metadado `_silver_ts` com o momento do processamento.
# MAGIC
# MAGIC Os nomes das colunas seguem os da fonte (em inglês), facilitando a rastreabilidade Bronze → Silver.
# MAGIC Única exceção: correção do erro de digitação `lenght` → `length` em `products`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F, Window, DataFrame

ACENTOS = "áàâãäéèêëíìîïóòôõöúùûüçñÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑ"
SEM_ACENTOS = "aaaaaeeeeiiiiooooouuuucnAAAAAEEEEIIIIOOOOOUUUUCN"


def bronze(nome: str) -> DataFrame:
    return spark.table(f"{BRONZE}.{nome}").drop("_ingestion_ts", "_source_file")


def limpar_cidade(col):
    """Minúsculas, sem acentos/diacríticos, apóstrofos padronizados e espaços normalizados."""
    c = F.lower(F.trim(col))
    c = F.translate(c, ACENTOS, SEM_ACENTOS)
    c = F.regexp_replace(c, r"\p{M}", "")          # remove marcas diacríticas soltas (ex.: 'são paulo')
    c = F.regexp_replace(c, "[´`’]", "'")
    return F.regexp_replace(c, r"\s+", " ")


def tc(coluna: str, tipo: str):
    """Conversão tolerante (try_cast): valor inválido vira nulo em vez de interromper o pipeline (modo ANSI)."""
    return F.expr(f"try_cast(`{coluna}` AS {tipo})")


def cep5(col):
    return F.lpad(F.trim(col), 5, "0")


def salvar(df: DataFrame, nome: str) -> None:
    (df.withColumn("_silver_ts", F.current_timestamp())
       .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"{SILVER}.{nome}"))
    print(f"{SILVER}.{nome:<22} {spark.table(f'{SILVER}.{nome}').count():>10,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## orders
# MAGIC - Datas convertidas para `TIMESTAMP`.
# MAGIC - Derivadas (somente para `order_status = 'delivered'` com data de entrega):
# MAGIC   - `delivery_days`: dias entre compra e entrega ao cliente;
# MAGIC   - `estimated_days`: dias entre compra e data estimada de entrega (prazo prometido);
# MAGIC   - `delay_days`: entrega − estimativa (positivo = atraso; negativo = antecipado);
# MAGIC   - `is_late`: entregue **em data posterior** à estimada (comparação por data, pois a estimativa não tem hora).
# MAGIC - `has_date_inconsistency`: sinaliza envio à transportadora antes da compra ou entrega antes do envio
# MAGIC   (189 casos na origem). O registro é mantido; o prazo compra → entrega não é afetado.

# COMMAND ----------

o = bronze("orders")
ts = lambda c: tc(c, "timestamp")
entregue = (F.col("order_status") == "delivered") & F.col("order_delivered_customer_date").isNotNull()

orders = (
    o.select(
        F.trim("order_id").alias("order_id"),
        F.trim("customer_id").alias("customer_id"),
        F.lower(F.trim("order_status")).alias("order_status"),
        ts("order_purchase_timestamp").alias("order_purchase_timestamp"),
        ts("order_approved_at").alias("order_approved_at"),
        ts("order_delivered_carrier_date").alias("order_delivered_carrier_date"),
        ts("order_delivered_customer_date").alias("order_delivered_customer_date"),
        ts("order_estimated_delivery_date").alias("order_estimated_delivery_date"),
    )
    .dropDuplicates(["order_id"])
    .withColumn("delivery_days", F.when(entregue, F.datediff(F.to_date("order_delivered_customer_date"), F.to_date("order_purchase_timestamp"))))
    .withColumn("estimated_days", F.datediff(F.to_date("order_estimated_delivery_date"), F.to_date("order_purchase_timestamp")))
    .withColumn("delay_days", F.when(entregue, F.datediff(F.to_date("order_delivered_customer_date"), F.to_date("order_estimated_delivery_date"))))
    .withColumn("is_late", F.when(entregue, F.to_date("order_delivered_customer_date") > F.to_date("order_estimated_delivery_date")))
    .withColumn("has_date_inconsistency",
                F.coalesce((F.col("order_delivered_carrier_date") < F.col("order_purchase_timestamp")) |
                           (F.col("order_delivered_customer_date") < F.col("order_delivered_carrier_date")), F.lit(False)))
)
salvar(orders, "orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## order_items
# MAGIC - `order_item_id` → INT; `price` e `freight_value` → `DECIMAL(12,2)` (valores monetários exatos).
# MAGIC - `shipping_limit_date` → TIMESTAMP.
# MAGIC - Frete = 0 é **mantido** (frete grátis é legítimo).

# COMMAND ----------

order_items = (
    bronze("order_items").select(
        F.trim("order_id").alias("order_id"),
        tc("order_item_id", "int").alias("order_item_id"),
        F.trim("product_id").alias("product_id"),
        F.trim("seller_id").alias("seller_id"),
        ts("shipping_limit_date").alias("shipping_limit_date"),
        tc("price", "decimal(12,2)").alias("price"),
        tc("freight_value", "decimal(12,2)").alias("freight_value"),
    )
    .dropDuplicates(["order_id", "order_item_id"])
)
salvar(order_items, "order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## order_payments
# MAGIC - Tipagem numérica; `payment_type` em minúsculas.
# MAGIC - `payment_installments = 0` (2 casos) → corrigido para **1** (pagamento à vista).
# MAGIC - `payment_type = 'not_defined'` (3 casos, pedidos cancelados) e valor 0 (vouchers) são mantidos.

# COMMAND ----------

order_payments = (
    bronze("order_payments").select(
        F.trim("order_id").alias("order_id"),
        tc("payment_sequential", "int").alias("payment_sequential"),
        F.lower(F.trim("payment_type")).alias("payment_type"),
        F.greatest(tc("payment_installments", "int"), F.lit(1)).alias("payment_installments"),
        tc("payment_value", "decimal(12,2)").alias("payment_value"),
    )
    .dropDuplicates(["order_id", "payment_sequential"])
)
salvar(order_payments, "order_payments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## order_reviews
# MAGIC - `review_score` → INT; registros com nota fora de 1–5 seriam descartados (nenhum encontrado).
# MAGIC - Títulos/comentários: `trim`, texto vazio → nulo; flag `has_comment`.
# MAGIC - **Deduplicação:** 551 pedidos têm mais de uma avaliação (202 com notas diferentes).
# MAGIC   Mantemos **uma avaliação por pedido**, a mais recente (`review_answer_timestamp`, depois `review_creation_date`),
# MAGIC   pois reflete a opinião final do cliente e evita contar o mesmo pedido duas vezes na análise de satisfação.

# COMMAND ----------

texto = lambda c: F.when(F.trim(F.col(c)) != "", F.trim(F.col(c)))
rv = (
    bronze("order_reviews").select(
        F.trim("review_id").alias("review_id"),
        F.trim("order_id").alias("order_id"),
        tc("review_score", "int").alias("review_score"),
        texto("review_comment_title").alias("review_comment_title"),
        texto("review_comment_message").alias("review_comment_message"),
        ts("review_creation_date").alias("review_creation_date"),
        ts("review_answer_timestamp").alias("review_answer_timestamp"),
    )
    .where(F.col("review_score").between(1, 5) & F.col("order_id").isNotNull())
)
w = Window.partitionBy("order_id").orderBy(F.desc_nulls_last("review_answer_timestamp"), F.desc_nulls_last("review_creation_date"), F.desc("review_id"))
order_reviews = (
    rv.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
      .withColumn("has_comment", F.col("review_comment_message").isNotNull())
)
salvar(order_reviews, "order_reviews")

# COMMAND ----------

# MAGIC %md
# MAGIC ## category_translation
# MAGIC Tabela de referência PT → EN. Duas categorias existentes em `products` não tinham tradução;
# MAGIC adicionadas manualmente para que nenhuma categoria fique sem nome em inglês.

# COMMAND ----------

traducoes_faltantes = spark.createDataFrame(
    [("pc_gamer", "pc_gamer"),
     ("portateis_cozinha_e_preparadores_de_alimentos", "portable_kitchen_and_food_preparers")],
    "product_category_name STRING, product_category_name_english STRING",
)
category_translation = (
    bronze("category_translation")
    .select(F.lower(F.trim("product_category_name")).alias("product_category_name"),
            F.lower(F.trim("product_category_name_english")).alias("product_category_name_english"))
    .unionByName(traducoes_faltantes)
    .dropDuplicates(["product_category_name"])
)
salvar(category_translation, "category_translation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## products
# MAGIC - Correção de nomes: `product_name_lenght` → `product_name_length`, `product_description_lenght` → `product_description_length`.
# MAGIC - Tipagem INT/DOUBLE.
# MAGIC - Categoria nula (610 produtos) → `sem_categoria` / `uncategorized` (evita perder receita no agrupamento por categoria).
# MAGIC - **JOIN** com `category_translation` por `product_category_name` para trazer o nome em inglês.
# MAGIC - `product_weight_g = 0` (4 produtos) → nulo (peso impossível).
# MAGIC - Derivada `product_volume_cm3` = comprimento × altura × largura.

# COMMAND ----------

p = bronze("products")
products = (
    p.select(
        F.trim("product_id").alias("product_id"),
        F.coalesce(F.lower(F.trim("product_category_name")), F.lit("sem_categoria")).alias("product_category_name"),
        tc("product_name_lenght", "int").alias("product_name_length"),
        tc("product_description_lenght", "int").alias("product_description_length"),
        tc("product_photos_qty", "int").alias("product_photos_qty"),
        F.when(tc("product_weight_g", "double") > 0, tc("product_weight_g", "double")).alias("product_weight_g"),
        tc("product_length_cm", "double").alias("product_length_cm"),
        tc("product_height_cm", "double").alias("product_height_cm"),
        tc("product_width_cm", "double").alias("product_width_cm"),
    )
    .dropDuplicates(["product_id"])
    .join(category_translation, "product_category_name", "left")
    .withColumn("product_category_name_english", F.coalesce("product_category_name_english", F.lit("uncategorized")))
    .withColumn("product_volume_cm3", F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm"))
)
salvar(products, "products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## customers e sellers
# MAGIC - CEP com 5 dígitos (`lpad`), UF em maiúsculas.
# MAGIC - Cidade padronizada (minúsculas, sem acentos).
# MAGIC - **sellers.seller_city** tinha valores sujos digitados pelos vendedores, ex.: `sao paulo / sao paulo`,
# MAGIC   `pinhais/pr`, `novo hamburgo, rio grande do sul, brasil`, `04482255`, e até um e-mail.
# MAGIC   Regra: manter apenas o trecho antes de `/`, `,` ou `\`; valores com dígitos ou `@` viram nulo;
# MAGIC   abreviação `sbc` → `sao bernardo do campo`.

# COMMAND ----------

customers = (
    bronze("customers").select(
        F.trim("customer_id").alias("customer_id"),
        F.trim("customer_unique_id").alias("customer_unique_id"),
        cep5(F.col("customer_zip_code_prefix")).alias("customer_zip_code_prefix"),
        limpar_cidade(F.col("customer_city")).alias("customer_city"),
        F.upper(F.trim("customer_state")).alias("customer_state"),
    )
    .dropDuplicates(["customer_id"])
)
salvar(customers, "customers")

cidade_vendedor = F.trim(F.regexp_extract(limpar_cidade(F.col("seller_city")), r"^([^/,\\]+)", 1))
sellers = (
    bronze("sellers").select(
        F.trim("seller_id").alias("seller_id"),
        cep5(F.col("seller_zip_code_prefix")).alias("seller_zip_code_prefix"),
        cidade_vendedor.alias("seller_city"),
        F.upper(F.trim("seller_state")).alias("seller_state"),
    )
    .withColumn("seller_city",
                F.when(F.col("seller_city").rlike("[0-9@]") | (F.col("seller_city") == ""), None)
                 .when(F.col("seller_city") == "sbc", "sao bernardo do campo")
                 .otherwise(F.col("seller_city")))
    .dropDuplicates(["seller_id"])
)
salvar(sellers, "sellers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## geolocation → geolocation (1 linha por prefixo de CEP)
# MAGIC - 261.831 linhas **idênticas** removidas; pontos fora do território brasileiro (42) removidos.
# MAGIC - A fonte tem vários pontos por prefixo de CEP (1.000.163 linhas para 19.015 prefixos). Para servir de
# MAGIC   referência de localização, agregamos para **uma linha por prefixo**: **mediana** de latitude e longitude
# MAGIC   (robusta a pontos isolados), cidade/UF mais frequentes e quantidade de pontos de origem.

# COMMAND ----------

g = (
    bronze("geolocation").select(
        cep5(F.col("geolocation_zip_code_prefix")).alias("zip_code_prefix"),
        tc("geolocation_lat", "double").alias("lat"),
        tc("geolocation_lng", "double").alias("lng"),
        limpar_cidade(F.col("geolocation_city")).alias("city"),
        F.upper(F.trim("geolocation_state")).alias("state"),
    )
    .dropDuplicates()
    .where(F.col("lat").between(BRASIL_LAT_MIN, BRASIL_LAT_MAX) & F.col("lng").between(BRASIL_LNG_MIN, BRASIL_LNG_MAX))
)
geolocation = g.groupBy("zip_code_prefix").agg(
    F.percentile_approx("lat", 0.5).alias("geolocation_lat"),
    F.percentile_approx("lng", 0.5).alias("geolocation_lng"),
    F.expr("mode(city)").alias("geolocation_city"),
    F.expr("mode(state)").alias("geolocation_state"),
    F.count("*").alias("source_points"),
).withColumnRenamed("zip_code_prefix", "geolocation_zip_code_prefix")
salvar(geolocation, "geolocation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência Bronze × Silver

# COMMAND ----------

comparacao = []
for tabela in ARQUIVOS_FONTE:
    b = spark.table(f"{BRONZE}.{tabela}").count()
    s = spark.table(f"{SILVER}.{tabela}").count()
    comparacao.append((tabela, b, s, b - s))
display(spark.createDataFrame(comparacao, "tabela STRING, linhas_bronze LONG, linhas_silver LONG, linhas_removidas LONG"))
