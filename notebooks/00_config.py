# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Configuração compartilhada
# MAGIC
# MAGIC Notebook de parâmetros reutilizado por todos os demais via `%run ./00_config`.
# MAGIC Centraliza nomes de catálogo/schemas, caminho do Volume de ingestão, lista de arquivos-fonte
# MAGIC e tabelas de referência estáticas (UF → Região).
# MAGIC
# MAGIC **Organização no Unity Catalog (arquitetura medalhão):**
# MAGIC
# MAGIC | Objeto | Papel |
# MAGIC |---|---|
# MAGIC | `olist.bronze` | Dados exatamente como recebidos (tudo `STRING`) + metadados de ingestão |
# MAGIC | `olist.bronze.landing` (Volume) | Área de pouso dos CSVs originais |
# MAGIC | `olist.silver` | Dados limpos, tipados, deduplicados e padronizados |
# MAGIC | `olist.gold` | Modelo dimensional (Esquema Estrela) pronto para análise |
# MAGIC | `olist.qualidade` | Resultados das verificações de qualidade de dados |

# COMMAND ----------

CATALOGO_PREFERIDO = "olist"


def _resolver_catalogo():
    """Cria (se necessário) e retorna o catálogo do projeto.

    Caso o workspace não permita criar catálogos, usa o catálogo padrão `workspace`,
    mantendo a mesma organização de schemas (bronze/silver/gold/qualidade).
    """
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOGO_PREFERIDO}")
        return CATALOGO_PREFERIDO
    except Exception as e:
        print(f"Aviso: não foi possível criar/usar o catálogo '{CATALOGO_PREFERIDO}' "
              f"({type(e).__name__}). Usando o catálogo 'workspace'.")
        return "workspace"


CATALOGO = _resolver_catalogo()
spark.sql(f"USE CATALOG {CATALOGO}")  # permite usar `bronze.x`, `silver.x`, `gold.x` nas células %sql

BRONZE = f"{CATALOGO}.bronze"
SILVER = f"{CATALOGO}.silver"
GOLD = f"{CATALOGO}.gold"
QUALIDADE = f"{CATALOGO}.qualidade"

VOLUME = f"{BRONZE}.landing"
VOLUME_PATH = f"/Volumes/{CATALOGO}/bronze/landing"

# COMMAND ----------

# Tabela Bronze -> arquivo CSV original do Kaggle (olistbr/brazilian-ecommerce)
ARQUIVOS_FONTE = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

# Quantidade de registros esperada em cada arquivo (conferida na origem, sem cabeçalho)
REGISTROS_ESPERADOS = {
    "customers": 99441,
    "geolocation": 1000163,
    "order_items": 112650,
    "order_payments": 103886,
    "order_reviews": 99224,
    "orders": 99441,
    "products": 32951,
    "sellers": 3095,
    "category_translation": 71,
}

# COMMAND ----------

# Referência estática: Unidade Federativa -> Região geográfica (IBGE)
UF_REGIAO = {
    "AC": "Norte", "AP": "Norte", "AM": "Norte", "PA": "Norte", "RO": "Norte", "RR": "Norte", "TO": "Norte",
    "AL": "Nordeste", "BA": "Nordeste", "CE": "Nordeste", "MA": "Nordeste", "PB": "Nordeste",
    "PE": "Nordeste", "PI": "Nordeste", "RN": "Nordeste", "SE": "Nordeste",
    "DF": "Centro-Oeste", "GO": "Centro-Oeste", "MT": "Centro-Oeste", "MS": "Centro-Oeste",
    "ES": "Sudeste", "MG": "Sudeste", "RJ": "Sudeste", "SP": "Sudeste",
    "PR": "Sul", "RS": "Sul", "SC": "Sul",
}
UFS_VALIDAS = sorted(UF_REGIAO.keys())

# Limites geográficos aproximados do território brasileiro (para validar latitude/longitude)
BRASIL_LAT_MIN, BRASIL_LAT_MAX = -33.75, 5.27
BRASIL_LNG_MIN, BRASIL_LNG_MAX = -73.99, -34.79

# COMMAND ----------

print(f"Catálogo: {CATALOGO}")
print(f"Schemas : {BRONZE} | {SILVER} | {GOLD} | {QUALIDADE}")
print(f"Volume  : {VOLUME_PATH}")
