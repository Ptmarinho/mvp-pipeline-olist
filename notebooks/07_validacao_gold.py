# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Validação de qualidade da Gold
# MAGIC
# MAGIC Depois das transformações, confirmamos que o modelo final é confiável:
# MAGIC - **Unicidade** das chaves primárias;
# MAGIC - **Integridade referencial** (nenhuma FK órfã);
# MAGIC - **Completude** das chaves;
# MAGIC - **Reconciliação** com a Silver (nenhum pedido, item ou valor perdido/duplicado pelos JOINs);
# MAGIC - **Domínios** das métricas (nota 1–5, prazos ≥ 0, valores ≥ 0).
# MAGIC
# MAGIC O notebook **falha** (assert) se alguma regra for violada, interrompendo o Job antes da análise.
# MAGIC Resultados em `olist.qualidade.validacao_gold`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

G = lambda t: f"{GOLD}.{t}"
S = lambda t: f"{SILVER}.{t}"


def orfas(fato, col, dim, dim_col):
    return f"SELECT count(*) FROM {G(fato)} f LEFT ANTI JOIN {G(dim)} d ON f.{col} = d.{dim_col} WHERE f.{col} IS NOT NULL"


REGRAS = [
    # (categoria, regra, SQL que retorna o nº de violações)
    ("Unicidade", "PK dim_data.data_key", f"SELECT count(*) - count(DISTINCT data_key) FROM {G('dim_data')}"),
    ("Unicidade", "PK dim_cliente.id_cliente", f"SELECT count(*) - count(DISTINCT id_cliente) FROM {G('dim_cliente')}"),
    ("Unicidade", "PK dim_vendedor.id_vendedor", f"SELECT count(*) - count(DISTINCT id_vendedor) FROM {G('dim_vendedor')}"),
    ("Unicidade", "PK dim_produto.id_produto", f"SELECT count(*) - count(DISTINCT id_produto) FROM {G('dim_produto')}"),
    ("Unicidade", "PK fato_pedidos.id_pedido", f"SELECT count(*) - count(DISTINCT id_pedido) FROM {G('fato_pedidos')}"),
    ("Unicidade", "PK fato_itens_pedido (id_pedido, num_item)", f"SELECT count(*) - count(DISTINCT id_pedido, num_item) FROM {G('fato_itens_pedido')}"),
    ("Integridade", "fato_pedidos.id_cliente → dim_cliente", orfas("fato_pedidos", "id_cliente", "dim_cliente", "id_cliente")),
    ("Integridade", "fato_pedidos.data_compra_key → dim_data", orfas("fato_pedidos", "data_compra_key", "dim_data", "data_key")),
    ("Integridade", "fato_pedidos.data_entrega_key → dim_data", orfas("fato_pedidos", "data_entrega_key", "dim_data", "data_key")),
    ("Integridade", "fato_pedidos.data_estimada_key → dim_data", orfas("fato_pedidos", "data_estimada_key", "dim_data", "data_key")),
    ("Integridade", "fato_itens_pedido.id_pedido → fato_pedidos", orfas("fato_itens_pedido", "id_pedido", "fato_pedidos", "id_pedido")),
    ("Integridade", "fato_itens_pedido.id_produto → dim_produto", orfas("fato_itens_pedido", "id_produto", "dim_produto", "id_produto")),
    ("Integridade", "fato_itens_pedido.id_vendedor → dim_vendedor", orfas("fato_itens_pedido", "id_vendedor", "dim_vendedor", "id_vendedor")),
    ("Integridade", "fato_itens_pedido.id_cliente → dim_cliente", orfas("fato_itens_pedido", "id_cliente", "dim_cliente", "id_cliente")),
    ("Integridade", "fato_itens_pedido.data_compra_key → dim_data", orfas("fato_itens_pedido", "data_compra_key", "dim_data", "data_key")),
    ("Completude", "Chaves obrigatórias não nulas na fato_itens_pedido",
     f"SELECT count_if(id_pedido IS NULL OR num_item IS NULL OR id_produto IS NULL OR id_vendedor IS NULL OR id_cliente IS NULL OR data_compra_key IS NULL) FROM {G('fato_itens_pedido')}"),
    ("Completude", "Chaves obrigatórias não nulas na fato_pedidos",
     f"SELECT count_if(id_pedido IS NULL OR id_cliente IS NULL OR data_compra_key IS NULL) FROM {G('fato_pedidos')}"),
    ("Reconciliação", "Nº de pedidos Gold = Silver", f"SELECT abs((SELECT count(*) FROM {G('fato_pedidos')}) - (SELECT count(*) FROM {S('orders')}))"),
    ("Reconciliação", "Nº de itens Gold = Silver", f"SELECT abs((SELECT count(*) FROM {G('fato_itens_pedido')}) - (SELECT count(*) FROM {S('order_items')}))"),
    ("Reconciliação", "Soma de preço Gold = Silver (R$)", f"SELECT abs((SELECT sum(preco) FROM {G('fato_itens_pedido')}) - (SELECT sum(price) FROM {S('order_items')}))"),
    ("Reconciliação", "Soma de valor pago Gold = Silver (R$)", f"SELECT abs((SELECT sum(valor_pago) FROM {G('fato_pedidos')}) - (SELECT sum(payment_value) FROM {S('order_payments')}))"),
    ("Reconciliação", "Soma de itens por pedido = total de itens",
     f"SELECT abs((SELECT sum(qtd_itens) FROM {G('fato_pedidos')}) - (SELECT count(*) FROM {G('fato_itens_pedido')}))"),
    ("Domínio", "nota_avaliacao entre 1 e 5", f"SELECT count_if(nota_avaliacao NOT BETWEEN 1 AND 5) FROM {G('fato_pedidos')}"),
    ("Domínio", "dias_entrega >= 0", f"SELECT count_if(dias_entrega < 0) FROM {G('fato_pedidos')}"),
    ("Domínio", "preco > 0 e valor_frete >= 0", f"SELECT count_if(preco <= 0 OR valor_frete < 0) FROM {G('fato_itens_pedido')}"),
    ("Domínio", "qtd_parcelas >= 1", f"SELECT count_if(qtd_parcelas < 1) FROM {G('fato_pedidos')}"),
    ("Domínio", "regiao preenchida em dim_cliente", f"SELECT count_if(regiao IS NULL) FROM {G('dim_cliente')}"),
]

resultados = []
for categoria, regra, sql in REGRAS:
    violacoes = int(spark.sql(sql).first()[0] or 0)
    resultados.append((categoria, regra, violacoes, "OK" if violacoes == 0 else "FALHA"))

df_val = spark.createDataFrame(resultados, "categoria STRING, regra STRING, violacoes LONG, status STRING")
df_val.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{QUALIDADE}.validacao_gold")
display(df_val)

# COMMAND ----------

falhas = [r for r in resultados if r[3] != "OK"]
assert not falhas, f"Validação da Gold falhou: {falhas}"
print(f"Todas as {len(resultados)} regras de validação da Gold passaram.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cobertura das informações usadas nas análises

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*)                                              AS pedidos,
# MAGIC   count_if(status_pedido = 'delivered')                 AS entregues,
# MAGIC   count_if(dias_entrega IS NOT NULL)                    AS com_prazo_calculado,
# MAGIC   count_if(nota_avaliacao IS NOT NULL)                  AS com_avaliacao,
# MAGIC   round(100 * count_if(nota_avaliacao IS NOT NULL) / count(*), 2) AS pct_com_avaliacao,
# MAGIC   count_if(qtd_itens = 0)                               AS sem_itens,
# MAGIC   count_if(flag_inconsistencia_datas)                   AS com_inconsistencia_datas
# MAGIC FROM gold.fato_pedidos
