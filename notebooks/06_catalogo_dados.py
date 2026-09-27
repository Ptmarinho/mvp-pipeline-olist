# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Catálogo de Dados no Unity Catalog
# MAGIC
# MAGIC Documentação **como código**: grava no Unity Catalog, para **todas as tabelas** das três camadas,
# MAGIC - a descrição da tabela (contexto, grão, origem);
# MAGIC - a descrição de cada coluna, com **domínio** (mín./máx. ou categorias) e **linhagem** (origem e transformação);
# MAGIC - as **chaves primárias e estrangeiras** do modelo Gold (constraints informativas, exibidas no Catalog Explorer).
# MAGIC
# MAGIC O tipo de dado de cada coluna é o próprio tipo físico da tabela Delta (visível no Catalog Explorer).
# MAGIC A linhagem tabela-a-tabela e coluna-a-coluna também é capturada automaticamente pelo Unity Catalog
# MAGIC (aba *Lineage*), pois todas as cargas são feitas por notebooks no Databricks.
# MAGIC
# MAGIC Deve ser executado **após** `05_gold` (as cargas com `overwrite` recriam o esquema das tabelas).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver

# COMMAND ----------

ORIGEM = {
    "orders": "olist_orders_dataset.csv", "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv", "order_reviews": "olist_order_reviews_dataset.csv",
    "customers": "olist_customers_dataset.csv", "sellers": "olist_sellers_dataset.csv",
    "products": "olist_products_dataset.csv", "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

SILVER_DOC = {
    "orders": ("Pedidos do marketplace Olist (set/2016 a out/2018), 1 linha por pedido, tipados, com métricas de prazo derivadas.", {
        "order_id": "Identificador único do pedido (hash, 32 caracteres). PK.",
        "customer_id": "Identificador do cliente no pedido (FK → customers). A Olist gera um customer_id por pedido.",
        "order_status": "Status do pedido. Domínio: delivered, shipped, canceled, unavailable, invoiced, processing, created, approved.",
        "order_purchase_timestamp": "Data/hora da compra. Domínio: 2016-09-04 a 2018-10-17.",
        "order_approved_at": "Data/hora da aprovação do pagamento. Nulo em 160 pedidos não aprovados.",
        "order_delivered_carrier_date": "Data/hora de postagem na transportadora. Nulo se não enviado.",
        "order_delivered_customer_date": "Data/hora de entrega ao cliente. Nulo se não entregue.",
        "order_estimated_delivery_date": "Data estimada de entrega informada ao cliente na compra (sem hora).",
        "delivery_days": "DERIVADA: dias entre compra e entrega (datediff). Só para status delivered. Domínio: 0 a 209.",
        "estimated_days": "DERIVADA: dias entre compra e data estimada (prazo prometido).",
        "delay_days": "DERIVADA: data de entrega − data estimada. Positivo = atraso; negativo = antecipado.",
        "is_late": "DERIVADA: true se entregue em data posterior à estimada. Nulo se não entregue.",
        "has_date_inconsistency": "DERIVADA: true se envio < compra ou entrega < envio (erro de registro na origem).",
    }),
    "order_items": ("Itens dos pedidos, 1 linha por item (order_id + order_item_id).", {
        "order_id": "Pedido (FK → orders). Parte da PK.",
        "order_item_id": "Número sequencial do item dentro do pedido. Domínio: 1 a 21. Parte da PK.",
        "product_id": "Produto vendido (FK → products).",
        "seller_id": "Vendedor que atendeu o item (FK → sellers).",
        "shipping_limit_date": "Data/hora limite para o vendedor entregar o item à transportadora.",
        "price": "Preço do item em R$ (DECIMAL). Domínio: 0,85 a 6.735,00.",
        "freight_value": "Frete do item em R$ (DECIMAL). Domínio: 0 (frete grátis) a 409,68.",
    }),
    "order_payments": ("Pagamentos dos pedidos, 1 linha por forma de pagamento usada no pedido.", {
        "order_id": "Pedido (FK → orders). Parte da PK.",
        "payment_sequential": "Sequência da forma de pagamento no pedido. Domínio: 1 a 29. Parte da PK.",
        "payment_type": "Forma de pagamento. Domínio: credit_card, boleto, voucher, debit_card, not_defined.",
        "payment_installments": "Número de parcelas. Domínio: 1 a 24 (0 na origem corrigido para 1).",
        "payment_value": "Valor pago nesta forma de pagamento em R$. Domínio: 0 a 13.664,08.",
    }),
    "order_reviews": ("Avaliações de satisfação, 1 linha por pedido (a mais recente; duplicatas removidas).", {
        "review_id": "Identificador da avaliação (pode se repetir entre pedidos na origem).",
        "order_id": "Pedido avaliado (FK → orders). Único nesta tabela.",
        "review_score": "Nota dada pelo cliente. Domínio: 1 a 5.",
        "review_comment_title": "Título do comentário (texto livre, PT). Nulo se não informado.",
        "review_comment_message": "Comentário do cliente (texto livre, PT). Nulo se não informado.",
        "review_creation_date": "Data de envio do questionário de satisfação ao cliente.",
        "review_answer_timestamp": "Data/hora da resposta do cliente.",
        "has_comment": "DERIVADA: true se o cliente escreveu comentário.",
    }),
    "customers": ("Clientes, 1 linha por customer_id, com localização padronizada.", {
        "customer_id": "Identificador do cliente no pedido. PK.",
        "customer_unique_id": "Identificador único da pessoa (agrupa compras recorrentes). 96.096 valores distintos.",
        "customer_zip_code_prefix": "5 primeiros dígitos do CEP do cliente (texto, com zeros à esquerda).",
        "customer_city": "Cidade do cliente (minúsculas, sem acentos).",
        "customer_state": "UF do cliente. Domínio: 27 UFs.",
    }),
    "sellers": ("Vendedores parceiros, 1 linha por seller_id, com cidade limpa.", {
        "seller_id": "Identificador do vendedor. PK.",
        "seller_zip_code_prefix": "5 primeiros dígitos do CEP do vendedor.",
        "seller_city": "Cidade do vendedor (limpa: sem UF/sufixos, sem acentos; inválidos → nulo).",
        "seller_state": "UF do vendedor. Domínio: 23 UFs presentes.",
    }),
    "products": ("Catálogo de produtos, 1 linha por product_id, com categoria traduzida (JOIN com category_translation).", {
        "product_id": "Identificador do produto. PK.",
        "product_category_name": "Categoria em português (73 categorias + 'sem_categoria' para nulos).",
        "product_category_name_english": "Categoria em inglês (JOIN com category_translation; 'uncategorized' para nulos).",
        "product_name_length": "Qtd. de caracteres do nome do produto (origem: product_name_lenght). Domínio: 5 a 76.",
        "product_description_length": "Qtd. de caracteres da descrição (origem: product_description_lenght). Domínio: 4 a 3.992.",
        "product_photos_qty": "Qtd. de fotos publicadas. Domínio: 1 a 20.",
        "product_weight_g": "Peso em gramas. Domínio: 2 a 40.425 (0 na origem → nulo).",
        "product_length_cm": "Comprimento em cm. Domínio: 7 a 105.",
        "product_height_cm": "Altura em cm. Domínio: 2 a 105.",
        "product_width_cm": "Largura em cm. Domínio: 6 a 118.",
        "product_volume_cm3": "DERIVADA: comprimento × altura × largura (cm³).",
    }),
    "geolocation": ("Referência geográfica, 1 linha por prefixo de CEP (agregada a partir de 1 milhão de pontos).", {
        "geolocation_zip_code_prefix": "Prefixo de CEP (5 dígitos). PK.",
        "geolocation_lat": "Mediana das latitudes dos pontos do prefixo. Domínio: -33,75 a 5,27 (Brasil).",
        "geolocation_lng": "Mediana das longitudes dos pontos do prefixo. Domínio: -73,99 a -34,79 (Brasil).",
        "geolocation_city": "Cidade mais frequente no prefixo (minúsculas, sem acentos).",
        "geolocation_state": "UF mais frequente no prefixo.",
        "source_points": "DERIVADA: quantidade de pontos distintos da origem agregados no prefixo.",
    }),
    "category_translation": ("Tradução das categorias de produto PT → EN (71 da origem + 2 adicionadas manualmente).", {
        "product_category_name": "Categoria em português. PK.",
        "product_category_name_english": "Categoria em inglês.",
    }),
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold

# COMMAND ----------

LIN_PED = "Origem: silver.orders"
GOLD_DOC = {
    "dim_data": ("Dimensão calendário, 1 linha por dia entre a primeira compra e a última data de entrega/estimativa. Gerada no pipeline (sequence).", {
        "data_key": "PK. Data no formato inteiro AAAAMMDD.",
        "data": "Data do calendário.",
        "ano": "Ano. Domínio: 2016 a 2018.",
        "trimestre": "Trimestre do ano. Domínio: 1 a 4.",
        "mes": "Mês numérico. Domínio: 1 a 12.",
        "nome_mes": "Nome do mês em português (Janeiro a Dezembro).",
        "ano_mes": "Ano-mês no formato AAAA-MM (útil para séries mensais).",
        "dia": "Dia do mês. Domínio: 1 a 31.",
        "dia_semana": "Dia da semana numérico. Domínio: 1 (Domingo) a 7 (Sábado).",
        "nome_dia_semana": "Nome do dia da semana em português.",
        "flag_fim_de_semana": "true se sábado ou domingo.",
        "flag_black_friday": "true se for a sexta-feira de Black Friday (sexta entre 23 e 29/nov).",
    }),
    "dim_cliente": ("Dimensão cliente, 1 linha por id_cliente. Origem: silver.customers + JOIN silver.geolocation (CEP) + região IBGE.", {
        "id_cliente": "PK. Origem: customers.customer_id.",
        "id_cliente_unico": "Identificador da pessoa (agrupa compras recorrentes). Origem: customers.customer_unique_id.",
        "cep_prefixo": "Prefixo de CEP (5 dígitos). Origem: customers.customer_zip_code_prefix.",
        "cidade": "Cidade (minúsculas, sem acentos). Origem: customers.customer_city.",
        "uf": "UF. Domínio: 27 UFs. Origem: customers.customer_state.",
        "regiao": "Região IBGE derivada da UF. Domínio: Norte, Nordeste, Centro-Oeste, Sudeste, Sul.",
        "latitude": "Latitude mediana do CEP. Origem: JOIN geolocation por CEP; nulo se CEP sem geolocalização.",
        "longitude": "Longitude mediana do CEP. Origem: JOIN geolocation por CEP; nulo se CEP sem geolocalização.",
    }),
    "dim_vendedor": ("Dimensão vendedor, 1 linha por id_vendedor. Origem: silver.sellers + JOIN silver.geolocation (CEP) + região IBGE.", {
        "id_vendedor": "PK. Origem: sellers.seller_id.",
        "cep_prefixo": "Prefixo de CEP (5 dígitos). Origem: sellers.seller_zip_code_prefix.",
        "cidade": "Cidade limpa. Origem: sellers.seller_city.",
        "uf": "UF. Domínio: 23 UFs presentes. Origem: sellers.seller_state.",
        "regiao": "Região IBGE derivada da UF.",
        "latitude": "Latitude mediana do CEP (JOIN geolocation).",
        "longitude": "Longitude mediana do CEP (JOIN geolocation).",
    }),
    "dim_produto": ("Dimensão produto, 1 linha por id_produto. Origem: silver.products (já com JOIN de tradução).", {
        "id_produto": "PK. Origem: products.product_id.",
        "categoria": "Categoria em português (74 valores incl. 'sem_categoria'). Origem: products.product_category_name.",
        "categoria_en": "Categoria em inglês. Origem: products.product_category_name_english.",
        "qtd_caracteres_nome": "Qtd. de caracteres do nome. Domínio: 5 a 76.",
        "qtd_caracteres_descricao": "Qtd. de caracteres da descrição. Domínio: 4 a 3.992.",
        "qtd_fotos": "Qtd. de fotos. Domínio: 1 a 20.",
        "peso_g": "Peso em gramas. Domínio: 2 a 40.425.",
        "comprimento_cm": "Comprimento em cm. Domínio: 7 a 105.",
        "altura_cm": "Altura em cm. Domínio: 2 a 105.",
        "largura_cm": "Largura em cm. Domínio: 6 a 118.",
        "volume_cm3": "Volume em cm³ (comprimento × altura × largura).",
    }),
    "fato_itens_pedido": ("Fato de itens vendidos. Grão: 1 linha por item de pedido. Origem: silver.order_items JOIN orders, customers, sellers, order_reviews.", {
        "id_pedido": "Parte da PK. Pedido (FK → fato_pedidos). Origem: order_items.order_id.",
        "num_item": "Parte da PK. Sequência do item no pedido. Domínio: 1 a 21. Origem: order_items.order_item_id.",
        "id_produto": "FK → dim_produto. Origem: order_items.product_id.",
        "id_vendedor": "FK → dim_vendedor. Origem: order_items.seller_id.",
        "id_cliente": "FK → dim_cliente. Origem: JOIN orders.customer_id.",
        "data_compra_key": "FK → dim_data. Data da compra (AAAAMMDD). Origem: JOIN orders.order_purchase_timestamp.",
        "status_pedido": "Status do pedido do item. Domínio: delivered, shipped, canceled, unavailable, invoiced, processing, created, approved.",
        "preco": "Preço do item em R$. Domínio: 0,85 a 6.735,00. Origem: order_items.price.",
        "valor_frete": "Frete do item em R$. Domínio: 0 a 409,68. Origem: order_items.freight_value.",
        "valor_total_item": "DERIVADA: preco + valor_frete (R$).",
        "flag_interestadual": "DERIVADA: true se UF do vendedor ≠ UF do cliente (JOIN sellers e customers).",
        "nota_avaliacao": "Nota (1 a 5) do pedido ao qual o item pertence. Origem: JOIN order_reviews; nulo se não avaliado.",
    }),
    "fato_pedidos": ("Fato de pedidos. Grão: 1 linha por pedido. Origem: silver.orders + agregações de order_items e order_payments + JOIN order_reviews.", {
        "id_pedido": "PK. Origem: orders.order_id.",
        "id_cliente": "FK → dim_cliente. Origem: orders.customer_id.",
        "data_compra_key": "FK → dim_data. Data da compra (AAAAMMDD).",
        "data_entrega_key": "FK → dim_data. Data da entrega ao cliente (AAAAMMDD); nulo se não entregue.",
        "data_estimada_key": "FK → dim_data. Data estimada de entrega (AAAAMMDD).",
        "status_pedido": "Status do pedido. Domínio: delivered, shipped, canceled, unavailable, invoiced, processing, created, approved.",
        "dt_compra": f"Data/hora da compra. {LIN_PED}.order_purchase_timestamp.",
        "dt_aprovacao": f"Data/hora da aprovação do pagamento. {LIN_PED}.order_approved_at.",
        "dt_envio_transportadora": f"Data/hora da postagem. {LIN_PED}.order_delivered_carrier_date.",
        "dt_entrega_cliente": f"Data/hora da entrega. {LIN_PED}.order_delivered_customer_date.",
        "dt_entrega_estimada": f"Data estimada de entrega. {LIN_PED}.order_estimated_delivery_date.",
        "dias_entrega": f"Dias entre compra e entrega (só entregues). Domínio: 0 a 209. {LIN_PED}.delivery_days.",
        "dias_prazo_estimado": f"Prazo prometido em dias (compra → estimativa). {LIN_PED}.estimated_days.",
        "dias_atraso": f"Entrega − estimativa em dias (positivo = atraso). {LIN_PED}.delay_days.",
        "flag_atrasado": f"true se entregue após a data estimada; nulo se não entregue. {LIN_PED}.is_late.",
        "flag_inconsistencia_datas": f"true se a sequência de datas da origem é inconsistente. {LIN_PED}.has_date_inconsistency.",
        "qtd_itens": "Qtd. de itens do pedido (COUNT de order_items; 0 se sem itens). Domínio: 0 a 21.",
        "qtd_vendedores": "Qtd. de vendedores distintos no pedido (COUNT DISTINCT seller_id).",
        "valor_produtos": "Soma de price dos itens (R$). Nulo se sem itens.",
        "valor_frete": "Soma de freight_value dos itens (R$). Nulo se sem itens.",
        "valor_pago": "Soma de payment_value dos pagamentos (R$). Origem: order_payments.",
        "tipo_pagamento_principal": "Forma de pagamento de maior valor no pedido. Domínio: credit_card, boleto, voucher, debit_card, not_defined.",
        "qtd_parcelas": "Maior número de parcelas entre os pagamentos do pedido. Domínio: 1 a 24.",
        "qtd_formas_pagamento": "Qtd. de formas de pagamento distintas no pedido.",
        "flag_interestadual": "true se ao menos um item foi vendido por vendedor de UF diferente do cliente.",
        "nota_avaliacao": "Nota da avaliação do pedido. Domínio: 1 a 5; nulo se não avaliado. Origem: order_reviews.review_score.",
        "flag_comentario": "true se a avaliação tem comentário escrito. Origem: order_reviews.has_comment.",
    }),
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicação dos comentários

# COMMAND ----------

esc = lambda s: s.replace("\\", "\\\\").replace("'", "\\'")


def comentar(tabela_fq, descricao_tabela, colunas):
    spark.sql(f"COMMENT ON TABLE {tabela_fq} IS '{esc(descricao_tabela)}'")
    existentes = set(spark.table(tabela_fq).columns)
    for col, desc in colunas.items():
        if col in existentes:
            spark.sql(f"ALTER TABLE {tabela_fq} ALTER COLUMN `{col}` COMMENT '{esc(desc)}'")
    faltando = existentes - set(colunas)
    print(f"{tabela_fq:<35} {len(existentes & set(colunas)):>3} colunas documentadas"
          + (f" | sem descrição: {sorted(faltando)}" if faltando else ""))


META_BRONZE = {
    "_ingestion_ts": "Metadado de controle: data/hora em que a linha foi ingerida na Bronze.",
    "_source_file": "Metadado de controle: caminho do arquivo CSV de origem no Volume landing.",
}
META_SILVER = {"_silver_ts": "Metadado de controle: data/hora do processamento na Silver."}

# Nomes de colunas da Bronze que diferem da Silver
BRONZE_PARA_SILVER = {"product_name_lenght": "product_name_length", "product_description_lenght": "product_description_length"}

for tabela, (desc, cols) in SILVER_DOC.items():
    comentar(f"{SILVER}.{tabela}", f"{desc} Origem: bronze.{tabela} ({ORIGEM[tabela]}).", {**cols, **META_SILVER})

    # Bronze: mesma semântica da Silver, porém valor bruto em STRING
    bronze_cols = {}
    for c in spark.table(f"{BRONZE}.{tabela}").columns:
        base = cols.get(BRONZE_PARA_SILVER.get(c, c))
        if base:
            bronze_cols[c] = f"Valor bruto (STRING, sem tratamento). {base.replace('DERIVADA: ', '')}"
    comentar(f"{BRONZE}.{tabela}",
             f"Dado bruto de {ORIGEM[tabela]} (Kaggle olistbr/brazilian-ecommerce), sem alteração; todas as colunas STRING.",
             {**bronze_cols, **META_BRONZE})

for tabela, (desc, cols) in GOLD_DOC.items():
    comentar(f"{GOLD}.{tabela}", desc, cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chaves primárias e estrangeiras (modelo estrela)
# MAGIC Constraints informativas do Unity Catalog: documentam o relacionamento e aparecem no Catalog Explorer
# MAGIC (não bloqueiam escrita; a integridade é validada em `07_validacao_gold`).

# COMMAND ----------

PKS = {
    "dim_data": ["data_key"],
    "dim_cliente": ["id_cliente"],
    "dim_vendedor": ["id_vendedor"],
    "dim_produto": ["id_produto"],
    "fato_pedidos": ["id_pedido"],
    "fato_itens_pedido": ["id_pedido", "num_item"],
}
FKS = [
    ("fato_pedidos", "id_cliente", "dim_cliente", "id_cliente"),
    ("fato_pedidos", "data_compra_key", "dim_data", "data_key"),
    ("fato_pedidos", "data_entrega_key", "dim_data", "data_key"),
    ("fato_pedidos", "data_estimada_key", "dim_data", "data_key"),
    ("fato_itens_pedido", "id_pedido", "fato_pedidos", "id_pedido"),
    ("fato_itens_pedido", "id_produto", "dim_produto", "id_produto"),
    ("fato_itens_pedido", "id_vendedor", "dim_vendedor", "id_vendedor"),
    ("fato_itens_pedido", "id_cliente", "dim_cliente", "id_cliente"),
    ("fato_itens_pedido", "data_compra_key", "dim_data", "data_key"),
]

for tabela, cols in PKS.items():
    for c in cols:
        spark.sql(f"ALTER TABLE {GOLD}.{tabela} ALTER COLUMN {c} SET NOT NULL")
    spark.sql(f"ALTER TABLE {GOLD}.{tabela} DROP CONSTRAINT IF EXISTS pk_{tabela} CASCADE")
    spark.sql(f"ALTER TABLE {GOLD}.{tabela} ADD CONSTRAINT pk_{tabela} PRIMARY KEY ({', '.join(cols)})")
    print(f"PK {tabela}({', '.join(cols)})")

for tabela, col, ref, ref_col in FKS:
    nome = f"fk_{tabela}_{col}"
    spark.sql(f"ALTER TABLE {GOLD}.{tabela} DROP CONSTRAINT IF EXISTS {nome}")
    spark.sql(f"ALTER TABLE {GOLD}.{tabela} ADD CONSTRAINT {nome} FOREIGN KEY ({col}) REFERENCES {GOLD}.{ref}({ref_col})")
    print(f"FK {tabela}.{col} → {ref}.{ref_col}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catálogo consolidado (consulta ao information_schema)
# MAGIC Visão tabular do catálogo gerado: tabela, coluna, tipo e descrição, útil para exportar e transcrever no README.

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_schema AS camada, table_name AS tabela, column_name AS coluna, full_data_type AS tipo, comment AS descricao
    FROM {CATALOGO}.information_schema.columns
    WHERE table_schema IN ('gold', 'silver')
    ORDER BY CASE table_schema WHEN 'gold' THEN 1 ELSE 2 END, table_name, ordinal_position
"""))
