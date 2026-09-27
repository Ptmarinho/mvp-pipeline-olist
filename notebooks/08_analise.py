# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Análise: respondendo às perguntas de negócio
# MAGIC
# MAGIC **Problema:** entender como a logística de entrega e a distribuição geográfica afetam a satisfação
# MAGIC dos clientes e o desempenho de vendas do marketplace Olist.
# MAGIC
# MAGIC Todas as consultas usam **somente a camada Gold** (modelo estrela). Cada pergunta tem:
# MAGIC consulta SQL → gráfico → discussão.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from pyspark.sql import functions as F

plt.rcParams.update({"figure.figsize": (11, 4.5), "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True, "grid.alpha": 0.3})
AZUL, LARANJA, CINZA = "#2563eb", "#ea580c", "#9ca3af"
q = lambda sql: spark.sql(sql)


def to_pd(df):
    """Converte para pandas transformando colunas DECIMAL em DOUBLE (o matplotlib não opera com Decimal)."""
    from pyspark.sql.types import DecimalType
    cols = [F.col(c.name).cast("double").alias(c.name) if isinstance(c.dataType, DecimalType) else F.col(c.name)
            for c in df.schema.fields]
    return df.select(cols).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## P1. O atraso na entrega reduz a nota de avaliação do cliente? Quanto?
# MAGIC Base: pedidos **entregues** e **avaliados**. Comparação entre entregues no prazo × com atraso e por faixa de dias de atraso.

# COMMAND ----------

p1 = q("""
SELECT CASE WHEN flag_atrasado THEN 'Com atraso' ELSE 'No prazo' END AS situacao,
       count(*)                                                AS pedidos,
       round(avg(nota_avaliacao), 2)                           AS nota_media,
       round(100 * avg(CASE WHEN nota_avaliacao <= 2 THEN 1 ELSE 0 END), 1) AS pct_notas_1_2,
       round(100 * avg(CASE WHEN nota_avaliacao = 5 THEN 1 ELSE 0 END), 1)  AS pct_nota_5
FROM gold.fato_pedidos
WHERE status_pedido = 'delivered' AND dias_entrega IS NOT NULL AND nota_avaliacao IS NOT NULL
GROUP BY 1 ORDER BY 1 DESC
""")
display(p1)

# COMMAND ----------

p1b = q("""
SELECT CASE WHEN dias_atraso <= 0 THEN '0. No prazo'
            WHEN dias_atraso <= 3 THEN '1. 1-3 dias'
            WHEN dias_atraso <= 7 THEN '2. 4-7 dias'
            WHEN dias_atraso <= 14 THEN '3. 8-14 dias'
            ELSE '4. 15+ dias' END                                AS faixa_atraso,
       count(*)                                                    AS pedidos,
       round(avg(nota_avaliacao), 2)                               AS nota_media,
       round(100 * avg(CASE WHEN nota_avaliacao <= 2 THEN 1 ELSE 0 END), 1) AS pct_notas_1_2
FROM gold.fato_pedidos
WHERE status_pedido = 'delivered' AND dias_entrega IS NOT NULL AND nota_avaliacao IS NOT NULL
GROUP BY 1 ORDER BY 1
""")
display(p1b)

pdf = to_pd(p1b)
fig, ax = plt.subplots()
barras = ax.bar(pdf["faixa_atraso"].str[3:], pdf["nota_media"], color=[AZUL] + [LARANJA] * (len(pdf) - 1))
ax.bar_label(barras, fmt="%.2f")
ax.set_ylim(0, 5); ax.set_ylabel("Nota média (1–5)"); ax.set_xlabel("Atraso em relação à data estimada")
ax.set_title("P1 · Nota média de avaliação por faixa de atraso na entrega")
plt.show()

print("Correlação (dias_atraso × nota):", q("""SELECT round(corr(dias_atraso, nota_avaliacao), 3) c FROM gold.fato_pedidos
      WHERE dias_entrega IS NOT NULL AND nota_avaliacao IS NOT NULL""").first()["c"])

# COMMAND ----------

# MAGIC %md
# MAGIC **Discussão P1.** Sim, e de forma intensa. Pedidos entregues no prazo têm nota média **≈ 4,29**, contra
# MAGIC **≈ 2,27** nos atrasados, uma queda de cerca de **2 pontos** na escala de 1 a 5. A proporção de notas ruins (1 ou 2)
# MAGIC sobe de **≈ 9%** para **≈ 62%**. O efeito é progressivo: 1 a 3 dias de atraso já derrubam a nota para ≈ 3,3,
# MAGIC e acima de 8 dias ela fica abaixo de 1,7. A correlação linear (≈ −0,27) é moderada porque a maioria dos pedidos
# MAGIC chega antes do prazo (a nota satura em 5), mas a relação por faixas é clara. **Cumprir o prazo prometido é o principal
# MAGIC fator logístico de satisfação.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## P2. Quais estados têm maior prazo médio de entrega e maior taxa de atraso?

# COMMAND ----------

p2 = q("""
SELECT c.uf, c.regiao,
       count(*)                                  AS pedidos_entregues,
       round(avg(f.dias_entrega), 1)             AS prazo_medio_dias,
       percentile_approx(f.dias_entrega, 0.5)    AS prazo_mediano_dias,
       round(avg(f.dias_prazo_estimado), 1)      AS prazo_prometido_medio,
       round(100 * avg(CAST(f.flag_atrasado AS INT)), 1) AS pct_atraso
FROM gold.fato_pedidos f
JOIN gold.dim_cliente c ON f.id_cliente = c.id_cliente
WHERE f.dias_entrega IS NOT NULL
GROUP BY c.uf, c.regiao
ORDER BY prazo_medio_dias DESC
""")
display(p2)

pdf = to_pd(p2)
fig, ax1 = plt.subplots(figsize=(13, 5))
ax1.bar(pdf["uf"], pdf["prazo_medio_dias"], color=AZUL, label="Prazo médio (dias)")
ax1.set_ylabel("Prazo médio de entrega (dias)")
ax2 = ax1.twinx()
ax2.plot(pdf["uf"], pdf["pct_atraso"], color=LARANJA, marker="o", label="% atraso")
ax2.set_ylabel("% de pedidos atrasados"); ax2.yaxis.set_major_formatter(mtick.PercentFormatter()); ax2.grid(False)
ax1.set_title("P2 · Prazo médio de entrega e taxa de atraso por UF do cliente")
fig.legend(loc="upper right", bbox_to_anchor=(0.9, 0.88))
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Discussão P2.** O prazo cresce com a distância do eixo Sul-Sudeste, onde está a maioria dos vendedores.
# MAGIC **RR (≈ 29 dias), AP (≈ 27) e AM (≈ 26)** têm os maiores prazos médios, contra **≈ 8,7 dias em SP** (média nacional ≈ 12,5).
# MAGIC A **taxa de atraso**, porém, não acompanha o prazo: os estados do Norte têm prazos longos, mas a Olist
# MAGIC **promete** prazos ainda mais longos (≈ 46 dias), e o atraso fica baixo (AP ≈ 3%, AM ≈ 3%). Os maiores atrasos estão no
# MAGIC **Nordeste e em RJ**: **AL (≈ 21%), MA (≈ 17%), SE (≈ 15%), PI e CE (≈ 14%)**, e **RJ (≈ 12%)** chama atenção por ser um
# MAGIC mercado grande com prazo médio de apenas 15 dias. Conclusão: o problema de satisfação não é o prazo longo em si,
# MAGIC mas **prometer um prazo e não cumprir**, o que se concentra no Nordeste e no RJ.

# COMMAND ----------

# MAGIC %md
# MAGIC ## P3. Quais categorias geram mais receita e quais têm pior avaliação média?

# COMMAND ----------

p3 = q("""
SELECT p.categoria,
       count(*)                                   AS itens_vendidos,
       round(sum(f.preco), 2)                     AS receita,
       round(100 * sum(f.preco) / sum(sum(f.preco)) OVER (), 2) AS pct_receita,
       round(avg(f.preco), 2)                     AS preco_medio,
       round(avg(f.nota_avaliacao), 2)            AS nota_media
FROM gold.fato_itens_pedido f
JOIN gold.dim_produto p ON f.id_produto = p.id_produto
GROUP BY p.categoria
ORDER BY receita DESC
""")
p3.createOrReplaceTempView("p3")
display(p3.limit(10))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Piores avaliações entre categorias relevantes (>= 500 itens vendidos, para evitar amostras pequenas)
# MAGIC SELECT categoria, itens_vendidos, receita, nota_media
# MAGIC FROM p3 WHERE itens_vendidos >= 500
# MAGIC ORDER BY nota_media ASC LIMIT 10

# COMMAND ----------

pdf = to_pd(p3.limit(10)).iloc[::-1]
fig, ax = plt.subplots(1, 2, figsize=(14, 5))
ax[0].barh(pdf["categoria"], pdf["receita"] / 1e6, color=AZUL)
ax[0].set_xlabel("Receita (R$ milhões)"); ax[0].set_title("Top 10 categorias por receita")
pior = to_pd(q("SELECT categoria, nota_media FROM p3 WHERE itens_vendidos >= 500 ORDER BY nota_media ASC LIMIT 10")).iloc[::-1]
ax[1].barh(pior["categoria"], pior["nota_media"], color=LARANJA)
ax[1].set_xlim(3, 4.5); ax[1].set_xlabel("Nota média"); ax[1].set_title("10 piores notas (categorias com ≥ 500 itens)")
fig.suptitle("P3 · Receita e satisfação por categoria"); plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Discussão P3.** A receita de produtos (≈ R$ 13,6 milhões) é concentrada: as **10 maiores categorias somam ≈ 62%**
# MAGIC do total, lideradas por **beleza_saude (≈ R$ 1,26 mi), relogios_presentes (≈ R$ 1,21 mi) e cama_mesa_banho (≈ R$ 1,04 mi)**.
# MAGIC Entre as categorias com volume relevante, as **piores notas** são de **moveis_escritorio (≈ 3,49)**, produtos
# MAGIC **sem_categoria (≈ 3,84)**, **cama_mesa_banho, moveis_sala e moveis_decoracao (≈ 3,90)** e **informatica_acessorios (≈ 3,93)**.
# MAGIC Há um padrão: **móveis** (itens volumosos, de montagem e frete mais difícil) concentram insatisfação. Destaque para
# MAGIC **cama_mesa_banho** e **informatica_acessorios**, que estão ao mesmo tempo no top 5 de receita e entre as piores notas,
# MAGIC sendo as categorias em que melhorar a experiência tem maior impacto financeiro. As melhores notas estão em livros, malas e papelaria (≥ 4,2).

# COMMAND ----------

# MAGIC %md
# MAGIC ## P4. Como evoluíram pedidos e receita mês a mês? Há sazonalidade (ex.: Black Friday)?
# MAGIC Meses incompletos no início (set–dez/2016) e no fim (set–out/2018) da base são **excluídos** para não distorcer a tendência.

# COMMAND ----------

p4 = q("""
SELECT d.ano_mes,
       count(DISTINCT f.id_pedido)  AS pedidos,
       round(sum(f.preco), 2)       AS receita,
       round(sum(f.preco) / count(DISTINCT f.id_pedido), 2) AS ticket_medio
FROM gold.fato_itens_pedido f
JOIN gold.dim_data d ON f.data_compra_key = d.data_key
WHERE d.ano_mes BETWEEN '2017-01' AND '2018-08'
GROUP BY d.ano_mes ORDER BY d.ano_mes
""")
display(p4)

pdf = to_pd(p4)
fig, ax = plt.subplots(figsize=(13, 4.5))
ax.plot(pdf["ano_mes"], pdf["receita"] / 1e3, color=AZUL, marker="o")
destaque = pdf[pdf["ano_mes"] == "2017-11"]
ax.scatter(destaque["ano_mes"], destaque["receita"] / 1e3, color=LARANJA, s=150, zorder=3, label="Nov/2017 (Black Friday)")
ax.set_ylabel("Receita (R$ mil)"); ax.set_title("P4 · Receita mensal de produtos"); ax.legend()
plt.xticks(rotation=45); plt.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dias com mais pedidos: efeito Black Friday
# MAGIC SELECT d.data, d.nome_dia_semana, d.flag_black_friday, count(*) AS pedidos
# MAGIC FROM gold.fato_pedidos f JOIN gold.dim_data d ON f.data_compra_key = d.data_key
# MAGIC GROUP BY d.data, d.nome_dia_semana, d.flag_black_friday
# MAGIC ORDER BY pedidos DESC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC **Discussão P4.** O marketplace teve **forte crescimento em 2017**: de ≈ 800 pedidos em jan/2017 para ≈ 7.500 em nov/2017.
# MAGIC Em **2018 o volume estabilizou** em um patamar de ≈ 6.200–7.300 pedidos e ≈ R$ 0,85–1,0 mi de receita por mês.
# MAGIC O pico é **novembro/2017** (≈ 7.544 pedidos, ≈ R$ 1,01 mi), puxado pela **Black Friday (24/11/2017)**, que teve
# MAGIC **1.176 pedidos em um único dia**, ≈ **7 vezes** a média diária do período (≈ 164) e o dia de maior volume da base.
# MAGIC Com apenas um ano completo não é possível afirmar um padrão sazonal anual, mas a Black Friday é um evento claro, que exige
# MAGIC planejamento de capacidade logística (e, dado P1, atenção redobrada ao cumprimento de prazo).

# COMMAND ----------

# MAGIC %md
# MAGIC ## P5. Pedidos interestaduais demoram mais, atrasam mais e pagam mais frete?
# MAGIC Base: pedidos entregues com **um único vendedor** (para que a origem do envio seja única).

# COMMAND ----------

p5 = q("""
SELECT CASE WHEN flag_interestadual THEN 'Interestadual' ELSE 'Mesma UF' END AS tipo_envio,
       count(*)                                     AS pedidos,
       round(100 * count(*) / sum(count(*)) OVER (), 1) AS pct_pedidos,
       round(avg(dias_entrega), 1)                  AS prazo_medio_dias,
       round(avg(dias_prazo_estimado), 1)           AS prazo_prometido_dias,
       round(100 * avg(CAST(flag_atrasado AS INT)), 1) AS pct_atraso,
       round(avg(valor_frete), 2)                   AS frete_medio,
       round(percentile_approx(100 * valor_frete / valor_produtos, 0.5), 1) AS frete_pct_produto_mediano,
       round(avg(nota_avaliacao), 2)                AS nota_media
FROM gold.fato_pedidos
WHERE dias_entrega IS NOT NULL AND qtd_vendedores = 1
GROUP BY 1 ORDER BY 1
""")
display(p5)

pdf = to_pd(p5).set_index("tipo_envio").loc[["Mesma UF", "Interestadual"]]
fig, ax = plt.subplots(1, 3, figsize=(14, 4))
for a, col, titulo in zip(ax, ["prazo_medio_dias", "pct_atraso", "frete_medio"], ["Prazo médio (dias)", "% atrasados", "Frete médio (R$)"]):
    b = a.bar(pdf.index, pdf[col], color=[CINZA, LARANJA]); a.bar_label(b, fmt="%.1f"); a.set_title(titulo)
fig.suptitle("P5 · Envio na mesma UF × interestadual"); plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Discussão P5.** Sim, nos três aspectos. **64%** dos pedidos são interestaduais. Eles levam **≈ 15,1 dias** contra
# MAGIC **≈ 7,9 dias** na mesma UF (quase o dobro), **atrasam mais** (≈ 8,1% × 4,6%) e pagam **frete ≈ 75% maior**
# MAGIC (≈ R$ 26,55 × R$ 15,19). O frete representa, na mediana, **≈ 25% do valor dos produtos** no interestadual, contra ≈ 18% na mesma UF.
# MAGIC A nota média também é menor (≈ 4,11 × 4,28). Como a maior parte dos vendedores está em SP, isso reforça P2: **atrair
# MAGIC vendedores fora do Sudeste** (ou usar centros de distribuição regionais) reduziria prazo, custo de frete e insatisfação.

# COMMAND ----------

# MAGIC %md
# MAGIC ## P6. Qual a forma de pagamento predominante e como o ticket médio varia com o número de parcelas?

# COMMAND ----------

p6 = q("""
SELECT tipo_pagamento_principal,
       count(*)                                         AS pedidos,
       round(100 * count(*) / sum(count(*)) OVER (), 1) AS pct_pedidos,
       round(sum(valor_pago), 2)                        AS valor_total,
       round(100 * sum(valor_pago) / sum(sum(valor_pago)) OVER (), 1) AS pct_valor,
       round(avg(valor_pago), 2)                        AS ticket_medio
FROM gold.fato_pedidos
WHERE tipo_pagamento_principal IS NOT NULL
GROUP BY 1 ORDER BY pedidos DESC
""")
display(p6)

p6b = q("""
SELECT CASE WHEN qtd_parcelas = 1 THEN '1. À vista (1x)'
            WHEN qtd_parcelas <= 3 THEN '2. 2-3x'
            WHEN qtd_parcelas <= 6 THEN '3. 4-6x'
            WHEN qtd_parcelas <= 9 THEN '4. 7-9x'
            ELSE '5. 10x ou mais' END      AS faixa_parcelas,
       count(*)                            AS pedidos,
       round(avg(valor_pago), 2)           AS ticket_medio,
       percentile_approx(valor_pago, 0.5)  AS ticket_mediano
FROM gold.fato_pedidos
WHERE tipo_pagamento_principal = 'credit_card'
GROUP BY 1 ORDER BY 1
""")
display(p6b)

a, b = to_pd(p6), to_pd(p6b)
fig, ax = plt.subplots(1, 2, figsize=(14, 4.5))
ax[0].pie(a["pedidos"], labels=a["tipo_pagamento_principal"], autopct=lambda v: f"{v:.1f}%" if v > 1 else "", startangle=90,
          colors=[AZUL, LARANJA, CINZA, "#16a34a", "#a855f7"][: len(a)])
ax[0].set_title("Pedidos por forma de pagamento principal")
bb = ax[1].bar(b["faixa_parcelas"].str[3:], b["ticket_medio"], color=AZUL); ax[1].bar_label(bb, fmt="R$ %.0f")
ax[1].set_title("Cartão de crédito: ticket médio por nº de parcelas"); ax[1].set_ylabel("R$")
fig.suptitle("P6 · Pagamentos"); plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Discussão P6.** O **cartão de crédito** domina: **≈ 75% dos pedidos e ≈ 78% do valor**, seguido do **boleto (≈ 20%)**;
# MAGIC voucher e débito somam menos de 5%. No cartão, **≈ 68% dos pedidos são parcelados**, e o ticket cresce
# MAGIC continuamente com o número de parcelas: de **≈ R$ 101 à vista** para **≈ R$ 415 em 10x ou mais** (≈ 4 vezes). O parcelamento
# MAGIC funciona como viabilizador de compras de maior valor; restringi-lo afetaria justamente os pedidos de ticket alto.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discussão geral
# MAGIC
# MAGIC As respostas se conectam em uma única narrativa sobre o problema proposto:
# MAGIC
# MAGIC 1. **A satisfação do cliente depende fortemente do cumprimento do prazo** (P1): o atraso derruba a nota média em ~2 pontos.
# MAGIC 2. **A geografia determina prazo e custo** (P2, P5): a concentração de vendedores no Sudeste faz pedidos interestaduais
# MAGIC    levarem o dobro do tempo, pagarem ~75% mais frete e atrasarem mais. O Nordeste e o RJ concentram os atrasos.
# MAGIC 3. **Categorias volumosas (móveis) e de alto volume (cama/mesa/banho, informática) têm as piores notas** (P3),
# MAGIC    sendo candidatas prioritárias a melhorias de embalagem, transporte e prazo prometido.
# MAGIC 4. **O negócio cresceu rápido em 2017 e estabilizou em 2018** (P4); a Black Friday gera um pico de ~7 vezes a demanda diária,
# MAGIC    um teste de estresse para a logística.
# MAGIC 5. **O crédito parcelado sustenta o ticket alto** (P6).
# MAGIC
# MAGIC **Recomendação:** priorizar a *precisão da promessa de entrega* (principalmente Nordeste/RJ e em picos como a Black Friday)
# MAGIC e a *descentralização da base de vendedores*, ações que atacam simultaneamente prazo, frete e satisfação.
