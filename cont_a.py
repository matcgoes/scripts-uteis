# Possuo uma base de dados que descreve a relação entre grupo economico e empresas, bem como informações do segmento da empresa, indicador de default da empresa, anomes da informação, etc.
# Um grupo economico é formado por uma ou mais empresas. Foi solicitado para que eu faça uma analise de "contaminação de atraso" entre os integrantes dos grupos.
# A contaminação é quando uma empresa do grupo atrasa e (quando nao estava em default no mes anterior e passou a estar no mes atual) consequentemente os outros integrantes também atrasam ao longo dos meses posteriores.
# Para isso, preciso partir da situação onde não há nenhum integrante do grupo em atraso, identificar caso onde houve atraso para então realizar a analise de contaminação.
# Gostaria também que esta analise de contaminação fosse observada entre os segmentos das empresas, por exemplo:
# Dado que um integrante do segmento 'SEG1' atrasou, como é a contaminação/taxa de contaminação nos demais integrantes do segmento 'SEG2' ? Quanto tempo demorou para que a houvesse a contaminação?
# 
# Segue o metadados da base:
# 
# cdccpfgru: id do grupo
# pk_riznumdoc_xx: id da empresa/integrante do grupo
# anomesref_score: anomes da informacao em AAAAMM
# segmento: segmento da empresa, podendo ser SEG1, SEG2, SEG3, SEG4, SEG5, SEG6, SEG7. Quanto menor, melhor o segmento. É uma informação atrelada à empresa/integrante. Nem todo grupo terá 
# integrantes de todos os segmentos.
# mau_origem: indicador se a empresa está em default na safra da informação. É uma informação atrelada à empresa/integrante.
# 
# Me mostre como você faria esta analise de contaminação, mostrando em como um determinado segmento do(s) integrante(s) em atraso afetam na contaminação dos demais integrantes e segmentos.
# 
# Descreva a logica, o passo a passo. Mostre em Python e SQL.

# ------------------
# Python
# -----------------

# Adicionar uma coluna com atraso no mês anterior
df['atraso_mes_anterior'] = df.groupby(['pk_riznumdoc_xx'])['mau_origem'].shift(1).fillna(0)

# Identificar início de atraso
df['inicio_atraso'] = (df['mau_origem'] == 1) & (df['atraso_mes_anterior'] == 0)

# Identificar o primeiro atraso por grupo
primeiro_atraso = df[df['inicio_atraso']].groupby('cdccpfgru')['anomesref_score'].min().reset_index()
primeiro_atraso.rename(columns={'anomesref_score': 'mes_primeiro_atraso'}, inplace=True)

# Mesclar com os dados originais
df = df.merge(primeiro_atraso, on='cdccpfgru', how='left')

# Criar coluna de janela de observação (meses desde o primeiro atraso)
df['meses_desde_atraso'] = (df['anomesref_score'] - df['mes_primeiro_atraso']) // 100 * 12 + \
                           (df['anomesref_score'] - df['mes_primeiro_atraso']) % 100

# Filtrar observação dentro da janela de 12 meses
janela_12_meses = df[(df['meses_desde_atraso'] > 0) & (df['meses_desde_atraso'] <= 12)]

# Taxa de contaminação por segmento
taxa_contaminacao = janela_12_meses.groupby(['segmento', 'cdccpfgru']).agg(
    contaminados=('mau_origem', 'sum'),
    total=('mau_origem', 'size')
).reset_index()
taxa_contaminacao['taxa_contaminacao'] = taxa_contaminacao['contaminados'] / taxa_contaminacao['total']

# Tempo médio de contaminação por segmento
tempo_contaminacao = janela_12_meses[janela_12_meses['mau_origem'] == 1].groupby(['segmento', 'cdccpfgru'])[
    'meses_desde_atraso'].mean().reset_index()
tempo_contaminacao.rename(columns={'meses_desde_atraso': 'tempo_medio_contaminacao'}, inplace=True)


# Identificar o segmento do integrante que iniciou o atraso no grupo
df['segmento_atraso_inicial'] = df.loc[df['inicio_atraso'], ['cdccpfgru', 'segmento']].set_index('cdccpfgru')['segmento']
df['segmento_atraso_inicial'] = df['cdccpfgru'].map(df['segmento_atraso_inicial'])

# Filtrar dados da janela de observação
janela_12_meses = df[(df['meses_desde_atraso'] > 0) & (df['meses_desde_atraso'] <= 12)]

# Marcar segmentos contaminados
janela_12_meses['contaminado'] = janela_12_meses['mau_origem'] == 1

# Contar contaminações cruzadas
contaminacao_cruzada = janela_12_meses.groupby(['segmento_atraso_inicial', 'segmento']).agg(
    contaminados=('contaminado', 'sum'),
    total=('contaminado', 'size')
).reset_index()

# Calcular taxa cruzada
contaminacao_cruzada['taxa_contaminacao_cruzada'] = contaminacao_cruzada['contaminados'] / contaminacao_cruzada['total']

# Calcular tempo médio de contaminação por segmento cruzado
tempo_cruzado = janela_12_meses[janela_12_meses['contaminado']].groupby(['segmento_atraso_inicial', 'segmento'])[
    'meses_desde_atraso'].mean().reset_index()
tempo_cruzado.rename(columns={'meses_desde_atraso': 'tempo_medio_contaminacao'}, inplace=True)

# Unir as métricas
contaminacao_cruzada = contaminacao_cruzada.merge(tempo_cruzado, on=['segmento_atraso_inicial', 'segmento'], how='left')


import seaborn as sns
import matplotlib.pyplot as plt

matriz = contaminacao_cruzada.pivot(
    index='segmento_atraso_inicial', columns='segmento', values='taxa_contaminacao_cruzada'
)

plt.figure(figsize=(10, 6))
sns.heatmap(matriz, annot=True, cmap='coolwarm', fmt=".2f")
plt.title("Taxa de Contaminação Cruzada entre Segmentos")
plt.show()


# ---------------
# SQL
# --------------

WITH atraso_anterior AS (
    SELECT 
        cdccpfgru,
        pk_riznumdoc_xx,
        anomesref_score,
        segmento,
        mau_origem,
        LAG(mau_origem) OVER (PARTITION BY pk_riznumdoc_xx ORDER BY anomesref_score) AS atraso_mes_anterior
    FROM base_dados
),
inicio_atraso AS (
    SELECT *,
        CASE 
            WHEN mau_origem = 1 AND (atraso_mes_anterior = 0 OR atraso_mes_anterior IS NULL) THEN 1
            ELSE 0
        END AS inicio_atraso
    FROM atraso_anterior
),
primeiro_atraso AS (
    SELECT 
        cdccpfgru,
        MIN(anomesref_score) AS mes_primeiro_atraso,
        MAX(segmento) AS segmento_atraso_inicial
    FROM inicio_atraso
    WHERE inicio_atraso = 1
    GROUP BY cdccpfgru
),
janela_observacao AS (
    SELECT 
        a.*,
        p.mes_primeiro_atraso,
        p.segmento_atraso_inicial,
        DATE_DIFF(
            DATE(DATE_TRUNC(CONCAT(LEFT(a.anomesref_score, 4), '-', RIGHT(a.anomesref_score, 2), '-01'), MONTH)),
            DATE(DATE_TRUNC(CONCAT(LEFT(p.mes_primeiro_atraso, 4), '-', RIGHT(p.mes_primeiro_atraso, 2), '-01'), MONTH)),
            MONTH
        ) AS meses_desde_atraso
    FROM atraso_anterior a
    JOIN primeiro_atraso p ON a.cdccpfgru = p.cdccpfgru
    WHERE DATE_DIFF(
        DATE(DATE_TRUNC(CONCAT(LEFT(a.anomesref_score, 4), '-', RIGHT(a.anomesref_score, 2), '-01'), MONTH)),
        DATE(DATE_TRUNC(CONCAT(LEFT(p.mes_primeiro_atraso, 4), '-', RIGHT(p.mes_primeiro_atraso, 2), '-01'), MONTH)),
        MONTH
    ) BETWEEN 1 AND 12
)
SELECT *
FROM janela_observacao;


