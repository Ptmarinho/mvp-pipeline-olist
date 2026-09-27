# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Qualidade de dados na Bronze
# MAGIC
# MAGIC Antes de transformar, medimos a qualidade do dado **como chegou**. Cada problema encontrado aqui
# MAGIC justifica uma regra de tratamento no notebook `04_silver`.
# MAGIC
# MAGIC Dimensões avaliadas para cada atributo:
# MAGIC
# MAGIC | Dimensão | Pergunta |
# MAGIC |---|---|
# MAGIC | **Completude** | Existem valores nulos ou vazios? Em que proporção? |
# MAGIC | **Consistência** | Os valores seguem o padrão esperado (datas, CEP com 5 dígitos, UF válida, domínios)? |
# MAGIC | **Unicidade** | Existem duplicatas onde não deveria haver (chaves primárias)? |
# MAGIC | **Acurácia** | Os valores fazem sentido no contexto (preço > 0, nota 1–5, coordenadas no Brasil, sequência de datas)? |
# MAGIC | **Integridade** | As chaves estrangeiras apontam para registros existentes? |
# MAGIC | **Outliers** | Há valores extremos (regra IQR: acima de Q3 + 1,5·IQR)? |
# MAGIC
# MAGIC Resultados persistidos em `olist.qualidade.perfil_completude_bronze` e `olist.qualidade.checagens_bronze`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

T = lambda nome: f"{BRONZE}.{nome}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Completude: nulos e vazios por atributo (todas as tabelas)

# COMMAND ----------

linhas = []
for tabela in ARQUIVOS_FONTE:
    df = spark.table(T(tabela))
    colunas = [c for c in df.columns if not c.startswith("_")]
    agg = df.agg(
        F.count(F.lit(1)).alias("__total"),
        *[F.count_if(F.col(c).isNull() | (F.trim(F.col(c)) == "")).alias(c) for c in colunas],
        *[F.countDistinct(c).alias(f"__dist_{c}") for c in colunas],
    ).first()
    for c in colunas:
        linhas.append((tabela, c, agg["__total"], agg[c], round(100 * agg[c] / agg["__total"], 2), agg[f"__dist_{c}"]))

df_completude = spark.createDataFrame(
    linhas, "tabela STRING, atributo STRING, total_linhas LONG, nulos_ou_vazios LONG, pct_nulos DOUBLE, valores_distintos LONG"
)
df_completude.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{QUALIDADE}.perfil_completude_bronze")
display(df_completude.orderBy(F.desc("pct_nulos"), "tabela", "atributo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Checagens de consistência, unicidade, acurácia e integridade
# MAGIC
# MAGIC Cada checagem conta quantos registros **falham** na regra e já registra o tratamento aplicado na Silver.

# COMMAND ----------

UFS_SQL = ",".join(f"'{u}'" for u in UFS_VALIDAS)
TS = lambda c: f"try_cast({c} AS TIMESTAMP)"

# (dimensão, tabela, atributo, regra, SQL que retorna colunas `falhas` e `total`, tratamento na Silver)
CHECAGENS = [
    # ---------------- Unicidade ----------------
    ("Unicidade", "orders", "order_id", "order_id deve ser único",
     f"SELECT count(*) - count(DISTINCT order_id) falhas, count(*) total FROM {T('orders')}", "Sem problema; PK mantida."),
    ("Unicidade", "customers", "customer_id", "customer_id deve ser único",
     f"SELECT count(*) - count(DISTINCT customer_id) falhas, count(*) total FROM {T('customers')}", "Sem problema; PK mantida."),
    ("Unicidade", "customers", "customer_unique_id", "Clientes recorrentes: um customer_unique_id com vários customer_id",
     f"SELECT count(*) - count(DISTINCT customer_unique_id) falhas, count(*) total FROM {T('customers')}",
     "Esperado (Olist gera um customer_id por pedido). Mantido; customer_unique_id identifica a pessoa."),
    ("Unicidade", "order_items", "order_id+order_item_id", "Chave composta do item deve ser única",
     f"SELECT count(*) - count(DISTINCT order_id, order_item_id) falhas, count(*) total FROM {T('order_items')}", "Sem problema; PK composta mantida."),
    ("Unicidade", "order_payments", "order_id+payment_sequential", "Chave composta do pagamento deve ser única",
     f"SELECT count(*) - count(DISTINCT order_id, payment_sequential) falhas, count(*) total FROM {T('order_payments')}", "Sem problema."),
    ("Unicidade", "order_reviews", "review_id", "review_id deveria ser único",
     f"SELECT count(*) - count(DISTINCT review_id) falhas, count(*) total FROM {T('order_reviews')}",
     "Mesma avaliação replicada para pedidos diferentes; chave passa a ser (review_id, order_id)."),
    ("Unicidade", "order_reviews", "order_id", "Cada pedido deveria ter uma única avaliação",
     f"SELECT count(*) - count(DISTINCT order_id) falhas, count(*) total FROM {T('order_reviews')}",
     "Mantida só a avaliação mais recente por pedido (maior review_answer_timestamp)."),
    ("Unicidade", "products", "product_id", "product_id deve ser único",
     f"SELECT count(*) - count(DISTINCT product_id) falhas, count(*) total FROM {T('products')}", "Sem problema."),
    ("Unicidade", "sellers", "seller_id", "seller_id deve ser único",
     f"SELECT count(*) - count(DISTINCT seller_id) falhas, count(*) total FROM {T('sellers')}", "Sem problema."),
    ("Unicidade", "geolocation", "linha inteira", "Linhas 100% idênticas",
     f"""SELECT count(*) - (SELECT count(*) FROM (SELECT DISTINCT geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
            geolocation_city, geolocation_state FROM {T('geolocation')})) falhas, count(*) total FROM {T('geolocation')}""",
     "Duplicatas removidas e tabela agregada para 1 linha por prefixo de CEP (mediana de lat/lng)."),
    ("Unicidade", "geolocation", "geolocation_zip_code_prefix", "Vários pontos por prefixo de CEP",
     f"SELECT count(*) - count(DISTINCT geolocation_zip_code_prefix) falhas, count(*) total FROM {T('geolocation')}",
     "Agregado para 1 linha por prefixo de CEP."),

    # ---------------- Consistência ----------------
    ("Consistência", "orders", "datas (5 colunas)", "Datas preenchidas devem ser timestamps válidos",
     f"""SELECT count_if(
            (order_purchase_timestamp IS NOT NULL AND {TS('order_purchase_timestamp')} IS NULL) OR
            (order_approved_at IS NOT NULL AND {TS('order_approved_at')} IS NULL) OR
            (order_delivered_carrier_date IS NOT NULL AND {TS('order_delivered_carrier_date')} IS NULL) OR
            (order_delivered_customer_date IS NOT NULL AND {TS('order_delivered_customer_date')} IS NULL) OR
            (order_estimated_delivery_date IS NOT NULL AND {TS('order_estimated_delivery_date')} IS NULL)) falhas,
            count(*) total FROM {T('orders')}""", "Conversão para TIMESTAMP."),
    ("Consistência", "orders", "order_status", "Status dentro do domínio conhecido",
     f"""SELECT count_if(order_status NOT IN ('delivered','shipped','canceled','unavailable','invoiced','processing','created','approved')) falhas,
            count(*) total FROM {T('orders')}""", "Sem problema."),
    ("Consistência", "customers", "customer_zip_code_prefix", "Prefixo de CEP com exatamente 5 dígitos",
     f"SELECT count_if(NOT customer_zip_code_prefix RLIKE '^[0-9]{{5}}$') falhas, count(*) total FROM {T('customers')}",
     "Padronização com lpad(5,'0') por segurança."),
    ("Consistência", "customers", "customer_state", "UF entre as 27 válidas",
     f"SELECT count_if(customer_state NOT IN ({UFS_SQL})) falhas, count(*) total FROM {T('customers')}", "Sem problema; upper()."),
    ("Consistência", "sellers", "seller_state", "UF entre as 27 válidas",
     f"SELECT count_if(seller_state NOT IN ({UFS_SQL})) falhas, count(*) total FROM {T('sellers')}", "Sem problema; upper()."),
    ("Consistência", "sellers", "seller_city", "Cidade apenas com letras (sem UF, barra, e-mail ou números)",
     f"SELECT count_if(seller_city RLIKE '[/\\\\\\\\,@0-9]' OR seller_city RLIKE '[^\\\\x00-\\\\x7F]') falhas, count(*) total FROM {T('sellers')}",
     "Remoção de acentos, corte em '/', ',' ou '\\\\', valores inválidos (e-mail/número) viram nulo."),
    ("Consistência", "geolocation", "geolocation_city", "Cidade sem acentos/caracteres especiais (padrão das demais tabelas)",
     f"SELECT count_if(geolocation_city RLIKE '[^\\\\x00-\\\\x7F]') falhas, count(*) total FROM {T('geolocation')}",
     "Remoção de acentos e marcas diacríticas (ex.: 'são paulo' -> 'sao paulo')."),
    ("Consistência", "order_payments", "payment_type", "Tipo de pagamento definido",
     f"SELECT count_if(payment_type = 'not_defined') falhas, count(*) total FROM {T('order_payments')}",
     "Mantido como 'not_defined' (3 registros, pedidos cancelados); não distorce a análise."),
    ("Consistência", "products", "product_category_name", "Categoria com tradução para inglês",
     f"""SELECT count_if(p.product_category_name IS NOT NULL AND t.product_category_name IS NULL) falhas, count(*) total
         FROM {T('products')} p LEFT JOIN {T('category_translation')} t USING (product_category_name)""",
     "Adicionadas 2 traduções faltantes (pc_gamer, portateis_cozinha_e_preparadores_de_alimentos)."),

    # ---------------- Acurácia ----------------
    ("Acurácia", "order_items", "price", "Preço deve ser > 0",
     f"SELECT count_if(try_cast(price AS DOUBLE) <= 0 OR try_cast(price AS DOUBLE) IS NULL) falhas, count(*) total FROM {T('order_items')}", "Sem problema."),
    ("Acurácia", "order_items", "freight_value", "Frete = 0 (frete grátis) ou negativo",
     f"SELECT count_if(try_cast(freight_value AS DOUBLE) <= 0) falhas, count(*) total FROM {T('order_items')}",
     "Frete 0 é válido (frete grátis); mantido. Nenhum negativo."),
    ("Acurácia", "order_payments", "payment_installments", "Parcelas devem ser >= 1",
     f"SELECT count_if(try_cast(payment_installments AS INT) < 1) falhas, count(*) total FROM {T('order_payments')}",
     "Parcelas 0 corrigidas para 1 (pagamento à vista)."),
    ("Acurácia", "order_payments", "payment_value", "Valor pago deve ser > 0",
     f"SELECT count_if(try_cast(payment_value AS DOUBLE) <= 0) falhas, count(*) total FROM {T('order_payments')}",
     "Mantidos (vouchers de valor zero); impacto desprezível."),
    ("Acurácia", "order_reviews", "review_score", "Nota entre 1 e 5",
     f"SELECT count_if(try_cast(review_score AS INT) NOT BETWEEN 1 AND 5 OR try_cast(review_score AS INT) IS NULL) falhas, count(*) total FROM {T('order_reviews')}",
     "Registros fora do domínio seriam descartados."),
    ("Acurácia", "products", "product_weight_g", "Peso deve ser > 0",
     f"SELECT count_if(try_cast(product_weight_g AS DOUBLE) <= 0) falhas, count(*) total FROM {T('products')}",
     "Peso 0 convertido para nulo (valor impossível)."),
    ("Acurácia", "geolocation", "lat/lng", "Coordenadas dentro do território brasileiro",
     f"""SELECT count_if(try_cast(geolocation_lat AS DOUBLE) NOT BETWEEN {BRASIL_LAT_MIN} AND {BRASIL_LAT_MAX}
                      OR try_cast(geolocation_lng AS DOUBLE) NOT BETWEEN {BRASIL_LNG_MIN} AND {BRASIL_LNG_MAX}) falhas,
            count(*) total FROM {T('geolocation')}""", "Pontos fora do Brasil removidos."),
    ("Acurácia", "orders", "order_delivered_customer_date", "Status 'delivered' deve ter data de entrega",
     f"SELECT count_if(order_status = 'delivered' AND order_delivered_customer_date IS NULL) falhas, count(*) total FROM {T('orders')}",
     "Mantidos; métricas de prazo ficam nulas para esses pedidos."),
    ("Acurácia", "orders", "order_delivered_customer_date", "Pedido não entregue não deveria ter data de entrega",
     f"SELECT count_if(order_status <> 'delivered' AND order_delivered_customer_date IS NOT NULL) falhas, count(*) total FROM {T('orders')}",
     "Métricas de entrega calculadas apenas para status 'delivered'."),
    ("Acurácia", "orders", "order_delivered_carrier_date", "Envio à transportadora antes da compra",
     f"SELECT count_if({TS('order_delivered_carrier_date')} < {TS('order_purchase_timestamp')}) falhas, count(*) total FROM {T('orders')}",
     "Sinalizado em has_date_inconsistency; não afeta prazo (compra -> entrega)."),
    ("Acurácia", "orders", "order_delivered_customer_date", "Entrega ao cliente antes do envio à transportadora",
     f"SELECT count_if({TS('order_delivered_customer_date')} < {TS('order_delivered_carrier_date')}) falhas, count(*) total FROM {T('orders')}",
     "Sinalizado em has_date_inconsistency."),

    # ---------------- Integridade referencial ----------------
    ("Integridade", "orders", "order_id", "Pedido sem nenhum item",
     f"SELECT count_if(i.order_id IS NULL) falhas, count(*) total FROM {T('orders')} o LEFT JOIN (SELECT DISTINCT order_id FROM {T('order_items')}) i USING (order_id)",
     "Pedidos mantidos na fato_pedidos com qtd_itens = 0 (quase todos 'unavailable'/'canceled')."),
    ("Integridade", "orders", "order_id", "Pedido sem pagamento",
     f"SELECT count_if(p.order_id IS NULL) falhas, count(*) total FROM {T('orders')} o LEFT JOIN (SELECT DISTINCT order_id FROM {T('order_payments')}) p USING (order_id)",
     "Mantido com valor_pago nulo."),
    ("Integridade", "orders", "order_id", "Pedido sem avaliação",
     f"SELECT count_if(r.order_id IS NULL) falhas, count(*) total FROM {T('orders')} o LEFT JOIN (SELECT DISTINCT order_id FROM {T('order_reviews')}) r USING (order_id)",
     "Mantido com nota_avaliacao nula; análises de nota consideram apenas pedidos avaliados."),
    ("Integridade", "orders", "customer_id", "customer_id existe em customers",
     f"SELECT count_if(c.customer_id IS NULL) falhas, count(*) total FROM {T('orders')} o LEFT JOIN {T('customers')} c USING (customer_id)", "Sem problema."),
    ("Integridade", "order_items", "product_id", "product_id existe em products",
     f"SELECT count_if(p.product_id IS NULL) falhas, count(*) total FROM {T('order_items')} i LEFT JOIN {T('products')} p USING (product_id)", "Sem problema."),
    ("Integridade", "order_items", "seller_id", "seller_id existe em sellers",
     f"SELECT count_if(s.seller_id IS NULL) falhas, count(*) total FROM {T('order_items')} i LEFT JOIN {T('sellers')} s USING (seller_id)", "Sem problema."),
    ("Integridade", "customers", "customer_zip_code_prefix", "CEP do cliente existe na geolocalização",
     f"""SELECT count_if(g.z IS NULL) falhas, count(*) total FROM {T('customers')} c
         LEFT JOIN (SELECT DISTINCT geolocation_zip_code_prefix z FROM {T('geolocation')}) g ON c.customer_zip_code_prefix = g.z""",
     "Clientes mantidos com latitude/longitude nulas."),
]

resultados = []
for dim, tabela, atributo, regra, sql, tratamento in CHECAGENS:
    r = spark.sql(sql).first()
    pct = round(100 * r["falhas"] / r["total"], 3) if r["total"] else 0.0
    resultados.append((dim, tabela, atributo, regra, int(r["falhas"]), int(r["total"]), pct,
                       "OK" if r["falhas"] == 0 else "PROBLEMA", tratamento))

df_checagens = spark.createDataFrame(
    resultados,
    "dimensao STRING, tabela STRING, atributo STRING, regra STRING, registros_com_falha LONG, "
    "total_registros LONG, pct_falha DOUBLE, status STRING, tratamento_silver STRING",
)
df_checagens.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{QUALIDADE}.checagens_bronze")
display(df_checagens)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Outliers (regra IQR)
# MAGIC
# MAGIC Outlier = valor acima de `Q3 + 1,5 × IQR`. Em e-commerce, preços e fretes altos são **legítimos**
# MAGIC (ex.: eletrodomésticos), então outliers são **mantidos**; as análises usam medianas quando a
# MAGIC média seria distorcida.

# COMMAND ----------

ALVOS_OUTLIER = [
    ("order_items", "price"),
    ("order_items", "freight_value"),
    ("order_payments", "payment_value"),
    ("products", "product_weight_g"),
]
linhas = []
for tabela, col in ALVOS_OUTLIER:
    df = spark.table(T(tabela)).select(F.expr(f"try_cast({col} AS DOUBLE)").alias("v")).where("v IS NOT NULL")
    q1, med, q3 = df.approxQuantile("v", [0.25, 0.5, 0.75], 0.001)
    limite = q3 + 1.5 * (q3 - q1)
    stats = df.agg(F.min("v").alias("mn"), F.max("v").alias("mx"), F.count_if(F.col("v") > limite).alias("out"), F.count("*").alias("n")).first()
    linhas.append((tabela, col, stats["mn"], q1, med, q3, stats["mx"], round(limite, 2), stats["out"], round(100 * stats["out"] / stats["n"], 2)))

# Prazo de entrega (dias entre compra e entrega) para pedidos entregues
dias = spark.sql(f"""
    SELECT datediff(to_date({TS('order_delivered_customer_date')}), to_date({TS('order_purchase_timestamp')})) AS v
    FROM {T('orders')} WHERE order_status = 'delivered' AND order_delivered_customer_date IS NOT NULL
""").select(F.col("v").cast("double").alias("v"))
q1, med, q3 = dias.approxQuantile("v", [0.25, 0.5, 0.75], 0.001)
limite = q3 + 1.5 * (q3 - q1)
s = dias.agg(F.min("v").alias("mn"), F.max("v").alias("mx"), F.count_if(F.col("v") > limite).alias("out"), F.count("*").alias("n")).first()
linhas.append(("orders", "dias_entrega (derivado)", s["mn"], q1, med, q3, s["mx"], round(limite, 2), s["out"], round(100 * s["out"] / s["n"], 2)))

df_outliers = spark.createDataFrame(
    linhas, "tabela STRING, atributo STRING, minimo DOUBLE, q1 DOUBLE, mediana DOUBLE, q3 DOUBLE, maximo DOUBLE, "
            "limite_superior_iqr DOUBLE, qtd_outliers LONG, pct_outliers DOUBLE"
)
df_outliers.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{QUALIDADE}.outliers_bronze")
display(df_outliers)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Resumo: problemas encontrados

# COMMAND ----------

display(df_checagens.where("status = 'PROBLEMA'").select("dimensao", "tabela", "atributo", "regra", "registros_com_falha", "pct_falha", "tratamento_silver"))
