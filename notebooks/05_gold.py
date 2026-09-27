# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Camada Gold: modelo dimensional (Esquema Estrela)
# MAGIC
# MAGIC Transforma a Silver em um **Esquema Estrela com duas tabelas fato** que compartilham dimensões
# MAGIC (constelação de fatos), cada fato com uma granularidade clara:
# MAGIC
# MAGIC | Tabela | Tipo | Grão (1 linha =) | Responde |
# MAGIC |---|---|---|---|
# MAGIC | `fato_pedidos` | Fato | um pedido | P1, P2, P4, P5, P6 (prazo, atraso, nota, pagamento) |
# MAGIC | `fato_itens_pedido` | Fato | um item de um pedido | P3, P4, P5 (receita por categoria/vendedor, frete) |
# MAGIC | `dim_cliente` | Dimensão | um customer_id | localização do cliente (UF, região, lat/lng) |
# MAGIC | `dim_vendedor` | Dimensão | um seller_id | localização do vendedor |
# MAGIC | `dim_produto` | Dimensão | um product_id | categoria (PT/EN) e atributos físicos |
# MAGIC | `dim_data` | Dimensão | um dia do calendário | ano, mês, trimestre, dia da semana, Black Friday |
# MAGIC
# MAGIC Nomes de tabelas e colunas em português, voltados ao consumo analítico.
# MAGIC As chaves primárias/estrangeiras são registradas como *constraints* no Unity Catalog pelo notebook `06_catalogo_dados`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F, Window

S = lambda nome: spark.table(f"{SILVER}.{nome}")

# Recarga completa: remove as tabelas Gold (fatos primeiro, por causa das chaves estrangeiras)
for t in ["fato_itens_pedido", "fato_pedidos", "dim_cliente", "dim_vendedor", "dim_produto", "dim_data"]:
    spark.sql(f"DROP TABLE IF EXISTS {GOLD}.{t}")


def salvar(df, nome):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD}.{nome}")
    print(f"{GOLD}.{nome:<20} {spark.table(f'{GOLD}.{nome}').count():>9,} linhas")


regiao = F.create_map(*[F.lit(x) for kv in UF_REGIAO.items() for x in kv])
data_key = lambda c: F.date_format(c, "yyyyMMdd").cast("int")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_data
# MAGIC Calendário contínuo do primeiro dia de compra até a última data (compra, entrega ou estimativa).
# MAGIC `data_key` no formato `AAAAMMDD` (inteiro). `flag_black_friday` = sexta-feira entre 23 e 29 de novembro.

# COMMAND ----------

limites = S("orders").select(
    F.min(F.to_date("order_purchase_timestamp")).alias("ini"),
    F.greatest(F.max(F.to_date("order_delivered_customer_date")), F.max(F.to_date("order_estimated_delivery_date"))).alias("fim"),
).first()

nomes_mes = F.array(*[F.lit(m) for m in ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho",
                                          "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]])
nomes_dia = F.array(*[F.lit(d) for d in ["Domingo", "Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado"]])

dim_data = (
    spark.sql(f"SELECT explode(sequence(DATE'{limites['ini']}', DATE'{limites['fim']}', INTERVAL 1 DAY)) AS data")
    .select(
        data_key("data").alias("data_key"),
        "data",
        F.year("data").alias("ano"),
        F.quarter("data").alias("trimestre"),
        F.month("data").alias("mes"),
        nomes_mes[F.month("data") - 1].alias("nome_mes"),
        F.date_format("data", "yyyy-MM").alias("ano_mes"),
        F.dayofmonth("data").alias("dia"),
        F.dayofweek("data").alias("dia_semana"),
        nomes_dia[F.dayofweek("data") - 1].alias("nome_dia_semana"),
        F.dayofweek("data").isin(1, 7).alias("flag_fim_de_semana"),
        ((F.month("data") == 11) & (F.dayofweek("data") == 6) & F.dayofmonth("data").between(23, 29)).alias("flag_black_friday"),
    )
)
salvar(dim_data, "dim_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_cliente e dim_vendedor
# MAGIC **JOIN** de `customers`/`sellers` com `geolocation` pelo prefixo de CEP para enriquecer com latitude/longitude
# MAGIC (LEFT JOIN: clientes cujo CEP não existe na geolocalização ficam com coordenadas nulas).
# MAGIC A região é derivada da UF pela tabela de referência do IBGE (`UF_REGIAO`).

# COMMAND ----------

geo = S("geolocation").select(
    F.col("geolocation_zip_code_prefix").alias("cep_prefixo"),
    F.col("geolocation_lat").alias("latitude"),
    F.col("geolocation_lng").alias("longitude"),
)

dim_cliente = (
    S("customers").select(
        F.col("customer_id").alias("id_cliente"),
        F.col("customer_unique_id").alias("id_cliente_unico"),
        F.col("customer_zip_code_prefix").alias("cep_prefixo"),
        F.col("customer_city").alias("cidade"),
        F.col("customer_state").alias("uf"),
        regiao[F.col("customer_state")].alias("regiao"),
    )
    .join(geo, "cep_prefixo", "left")
    .select("id_cliente", "id_cliente_unico", "cep_prefixo", "cidade", "uf", "regiao", "latitude", "longitude")
)
salvar(dim_cliente, "dim_cliente")

dim_vendedor = (
    S("sellers").select(
        F.col("seller_id").alias("id_vendedor"),
        F.col("seller_zip_code_prefix").alias("cep_prefixo"),
        F.col("seller_city").alias("cidade"),
        F.col("seller_state").alias("uf"),
        regiao[F.col("seller_state")].alias("regiao"),
    )
    .join(geo, "cep_prefixo", "left")
    .select("id_vendedor", "cep_prefixo", "cidade", "uf", "regiao", "latitude", "longitude")
)
salvar(dim_vendedor, "dim_vendedor")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_produto
# MAGIC Produto com categoria em português e inglês (já traduzida na Silver) e atributos físicos.

# COMMAND ----------

dim_produto = S("products").select(
    F.col("product_id").alias("id_produto"),
    F.col("product_category_name").alias("categoria"),
    F.col("product_category_name_english").alias("categoria_en"),
    F.col("product_name_length").alias("qtd_caracteres_nome"),
    F.col("product_description_length").alias("qtd_caracteres_descricao"),
    F.col("product_photos_qty").alias("qtd_fotos"),
    F.col("product_weight_g").alias("peso_g"),
    F.col("product_length_cm").alias("comprimento_cm"),
    F.col("product_height_cm").alias("altura_cm"),
    F.col("product_width_cm").alias("largura_cm"),
    F.col("product_volume_cm3").alias("volume_cm3"),
)
salvar(dim_produto, "dim_produto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fato_itens_pedido (grão: item do pedido)
# MAGIC - Base: `silver.order_items`.
# MAGIC - **JOIN** com `orders` (por `order_id`) para trazer cliente, data da compra e status.
# MAGIC - **JOIN** com `customers` e `sellers` para calcular `flag_interestadual` (UF do vendedor ≠ UF do cliente).
# MAGIC - **JOIN** com `order_reviews` (1 avaliação por pedido) para trazer a nota do pedido ao item, permitindo avaliar categorias.

# COMMAND ----------

ped = S("orders").select("order_id", "customer_id", "order_status", "order_purchase_timestamp")
uf_cli = S("customers").select("customer_id", F.col("customer_state").alias("uf_cliente"))
uf_ven = S("sellers").select("seller_id", F.col("seller_state").alias("uf_vendedor"))
notas = S("order_reviews").select("order_id", "review_score")

itens = (
    S("order_items")
    .join(ped, "order_id", "inner")
    .join(uf_cli, "customer_id", "left")
    .join(uf_ven, "seller_id", "left")
    .join(notas, "order_id", "left")
)

fato_itens_pedido = itens.select(
    F.col("order_id").alias("id_pedido"),
    F.col("order_item_id").alias("num_item"),
    F.col("product_id").alias("id_produto"),
    F.col("seller_id").alias("id_vendedor"),
    F.col("customer_id").alias("id_cliente"),
    data_key(F.col("order_purchase_timestamp")).alias("data_compra_key"),
    F.col("order_status").alias("status_pedido"),
    F.col("price").alias("preco"),
    F.col("freight_value").alias("valor_frete"),
    (F.col("price") + F.col("freight_value")).cast("decimal(12,2)").alias("valor_total_item"),
    (F.col("uf_vendedor") != F.col("uf_cliente")).alias("flag_interestadual"),
    F.col("review_score").alias("nota_avaliacao"),
)
salvar(fato_itens_pedido, "fato_itens_pedido")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fato_pedidos (grão: pedido)
# MAGIC - Base: `silver.orders` (todos os 99.441 pedidos, inclusive sem itens, com `qtd_itens = 0`).
# MAGIC - Agregação de **itens** por pedido: quantidade, nº de vendedores, valor de produtos e frete, se algum item é interestadual.
# MAGIC - Agregação de **pagamentos** por pedido: valor pago total, nº de parcelas (máximo), nº de formas de pagamento e
# MAGIC   **forma principal** (a de maior valor no pedido).
# MAGIC - **JOIN** com a avaliação (1 por pedido).

# COMMAND ----------

agg_itens = spark.table(f"{GOLD}.fato_itens_pedido").groupBy(F.col("id_pedido").alias("order_id")).agg(
    F.count("*").cast("int").alias("qtd_itens"),
    F.countDistinct("id_vendedor").cast("int").alias("qtd_vendedores"),
    F.sum("preco").cast("decimal(12,2)").alias("valor_produtos"),
    F.sum("valor_frete").cast("decimal(12,2)").alias("valor_frete"),
    F.max(F.col("flag_interestadual").cast("int")).cast("boolean").alias("flag_interestadual"),
)

pg = S("order_payments")
w = Window.partitionBy("order_id").orderBy(F.desc("payment_value"), F.asc("payment_sequential"))
forma_principal = pg.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").select("order_id", F.col("payment_type").alias("tipo_pagamento_principal"))
agg_pag = pg.groupBy("order_id").agg(
    F.sum("payment_value").cast("decimal(12,2)").alias("valor_pago"),
    F.max("payment_installments").alias("qtd_parcelas"),
    F.countDistinct("payment_type").cast("int").alias("qtd_formas_pagamento"),
).join(forma_principal, "order_id", "left")

rv = S("order_reviews").select("order_id", F.col("review_score").alias("nota_avaliacao"), F.col("has_comment").alias("flag_comentario"))

fato_pedidos = (
    S("orders")
    .join(agg_itens, "order_id", "left")
    .join(agg_pag, "order_id", "left")
    .join(rv, "order_id", "left")
    .select(
        F.col("order_id").alias("id_pedido"),
        F.col("customer_id").alias("id_cliente"),
        data_key(F.col("order_purchase_timestamp")).alias("data_compra_key"),
        data_key(F.col("order_delivered_customer_date")).alias("data_entrega_key"),
        data_key(F.col("order_estimated_delivery_date")).alias("data_estimada_key"),
        F.col("order_status").alias("status_pedido"),
        F.col("order_purchase_timestamp").alias("dt_compra"),
        F.col("order_approved_at").alias("dt_aprovacao"),
        F.col("order_delivered_carrier_date").alias("dt_envio_transportadora"),
        F.col("order_delivered_customer_date").alias("dt_entrega_cliente"),
        F.col("order_estimated_delivery_date").alias("dt_entrega_estimada"),
        F.col("delivery_days").alias("dias_entrega"),
        F.col("estimated_days").alias("dias_prazo_estimado"),
        F.col("delay_days").alias("dias_atraso"),
        F.col("is_late").alias("flag_atrasado"),
        F.col("has_date_inconsistency").alias("flag_inconsistencia_datas"),
        F.coalesce("qtd_itens", F.lit(0)).alias("qtd_itens"),
        F.coalesce("qtd_vendedores", F.lit(0)).alias("qtd_vendedores"),
        "valor_produtos",
        "valor_frete",
        "valor_pago",
        "tipo_pagamento_principal",
        "qtd_parcelas",
        "qtd_formas_pagamento",
        "flag_interestadual",
        "nota_avaliacao",
        "flag_comentario",
    )
)
salvar(fato_pedidos, "fato_pedidos")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exemplo de consulta no modelo estrela: receita e nota média por região do cliente
# MAGIC SELECT c.regiao,
# MAGIC        count(DISTINCT f.id_pedido)          AS pedidos,
# MAGIC        round(sum(f.preco), 2)               AS receita_produtos,
# MAGIC        round(avg(f.nota_avaliacao), 2)      AS nota_media
# MAGIC FROM gold.fato_itens_pedido f
# MAGIC JOIN gold.dim_cliente c ON f.id_cliente = c.id_cliente
# MAGIC GROUP BY c.regiao
# MAGIC ORDER BY receita_produtos DESC
