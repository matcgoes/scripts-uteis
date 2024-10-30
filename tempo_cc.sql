-- CLAUDE AI



WITH 
-- Primeiro, vamos normalizar as datas de encerramento, substituindo missing e '9999-12-31' por data atual
dados_normalizados AS (
    SELECT 
        id_cliente,
        numero_agencia,
        numero_conta,
        digito_verificador_conta,
        date_parse(dat_abet_cont, '%Y-%m-%d') as data_abertura,
        CASE 
            WHEN dat_encerramento_cont IS NULL OR dat_encerramento_cont = '9999-12-31' 
            THEN current_date
            ELSE date_parse(dat_encerramento_cont, '%Y-%m-%d')
        END as data_encerramento
    FROM sua_tabela
),

-- Encontrar primeira e última conta por cliente
datas_extremas AS (
    SELECT 
        id_cliente,
        MIN(data_abertura) as primeira_abertura,
        MAX(CASE 
            WHEN data_encerramento = current_date THEN NULL 
            ELSE data_encerramento 
        END) as ultimo_encerramento,
        MIN(CASE 
            WHEN data_encerramento = current_date THEN data_abertura 
            ELSE NULL 
        END) as primeira_abertura_ativa,
        MAX(CASE 
            WHEN data_encerramento = current_date THEN data_abertura 
            ELSE NULL 
        END) as ultima_abertura_ativa
    FROM dados_normalizados
    GROUP BY id_cliente
),

-- Criar ranges de datas por cliente para análise de sobreposição
ranges_conta AS (
    SELECT 
        id_cliente,
        data_abertura as data_evento,
        1 as tipo_evento  -- 1 para abertura
    FROM dados_normalizados
    
    UNION ALL
    
    SELECT 
        id_cliente,
        data_encerramento as data_evento,
        -1 as tipo_evento  -- -1 para encerramento
    FROM dados_normalizados
    WHERE data_encerramento != current_date
),

-- Calcular períodos ativos (considerando sobreposições)
periodos_ativos AS (
    SELECT 
        id_cliente,
        data_evento,
        LEAD(data_evento) OVER (PARTITION BY id_cliente ORDER BY data_evento) as proxima_data,
        SUM(tipo_evento) OVER (PARTITION BY id_cliente ORDER BY data_evento) as contas_ativas
    FROM ranges_conta
),

-- Calcular tempo efetivo de relacionamento (excluindo gaps)
tempo_efetivo AS (
    SELECT 
        id_cliente,
        SUM(
            CASE 
                WHEN contas_ativas > 0 AND proxima_data IS NOT NULL 
                THEN DATE_DIFF('month', data_evento, proxima_data)
                WHEN contas_ativas > 0 AND proxima_data IS NULL 
                THEN DATE_DIFF('month', data_evento, current_date)
                ELSE 0 
            END
        ) as meses_ativos
    FROM periodos_ativos
    GROUP BY id_cliente
)

-- Resultado final
SELECT 
    d.id_cliente,
    -- Tempo total desde primeira conta até hoje ou último encerramento
    DATE_DIFF('month', 
        d.primeira_abertura, 
        COALESCE(NULLIF(d.ultimo_encerramento, current_date), current_date)
    ) as tempo_total_meses,
    -- Tempo efetivo apenas de períodos ativos
    t.meses_ativos as tempo_ativo_meses,
    -- Datas importantes
    d.primeira_abertura,
    d.ultimo_encerramento,
    d.primeira_abertura_ativa,
    d.ultima_abertura_ativa
FROM datas_extremas d
LEFT JOIN tempo_efetivo t ON d.id_cliente = t.id_cliente









-- GPT

WITH contas_formatadas AS (
    -- Converte os campos de data para formato de data e define o encerramento para contas abertas atualmente
    SELECT 
        id_cliente,
        numero_agencia,
        numero_conta,
        digito_verificador_conta,
        DATE_PARSE(dat_abet_cont, '%Y-%m-%d') AS dat_abet_cont,
        COALESCE(
            NULLIF(DATE_PARSE(dat_encerramento_cont, '%Y-%m-%d'), DATE '9999-12-31'), 
            current_date
        ) AS dat_encerramento_cont
    FROM contas_clientes
),

contas_ordenadas AS (
    -- Ordena cada conta por cliente para simplificar a comparação de períodos
    SELECT
        id_cliente,
        dat_abet_cont,
        dat_encerramento_cont,
        ROW_NUMBER() OVER(PARTITION BY id_cliente ORDER BY dat_abet_cont) AS row_num
    FROM contas_formatadas
),

periodos_agrupados AS (
    -- Define períodos de relacionamento contínuo consolidando as contas sobrepostas e consecutivas
    SELECT
        id_cliente,
        MIN(dat_abet_cont) AS dat_abet_cont,
        MAX(dat_encerramento_cont) AS dat_encerramento_cont
    FROM (
        SELECT 
            id_cliente,
            dat_abet_cont,
            dat_encerramento_cont,
            SUM(CASE WHEN dat_abet_cont <= LAG(dat_encerramento_cont) OVER (PARTITION BY id_cliente ORDER BY dat_abet_cont)
                     THEN 0 ELSE 1 END) OVER (PARTITION BY id_cliente ORDER BY dat_abet_cont) AS grupo
        FROM contas_ordenadas
    ) agrupado
    GROUP BY id_cliente, grupo
),

tempo_relacionamento AS (
    -- Calcula o tempo de relacionamento (ativo e total) para cada cliente
    SELECT
        id_cliente,
        -- Tempo total de relacionamento (em meses) desde a primeira abertura até o último encerramento
        DATE_DIFF('month', MIN(dat_abet_cont), MAX(dat_encerramento_cont)) AS tempo_total_meses,
        
        -- Tempo ativo (soma dos períodos únicos em meses)
        SUM(DATE_DIFF('month', dat_abet_cont, dat_encerramento_cont)) AS tempo_ativo_meses,
        
        -- Informações adicionais solicitadas
        MIN(dat_abet_cont) AS dat_primeira_abertura,
        MAX(dat_encerramento_cont) AS dat_ultimo_encerramento,
        MAX(dat_abet_cont) AS dat_ultima_abertura,
        MIN(dat_encerramento_cont) AS dat_primeiro_encerramento
    FROM periodos_agrupados
    GROUP BY id_cliente
)

-- Resultado Final
SELECT * FROM tempo_relacionamento;



-----------------------------------------------------------------------------------------------------------------------------------------------------------


--- VERSÃO 2


-- CLAUDE AI V2

WITH 
-- Tratamento inicial dos dados, normalizando datas e segmentos
base_normalizada AS (
    SELECT 
        cpf_raiz,
        agencia,
        conta,
        dac,
        sgto,
        -- Ordem de prioridade dos segmentos
        CASE 
            WHEN sgto = '4' THEN 3
            WHEN sgto = 'L' THEN 2
            WHEN sgto = '3' THEN 1
            ELSE 0
        END as sgto_ordem,
        -- Tratamento data de abertura
        CAST(dat_abet_cc AS date) as data_abertura,
        -- Tratamento data de encerramento
        CASE 
            WHEN dat_encr_cc IS NULL OR dat_encr_cc = '9999-12-31' THEN CURRENT_DATE
            ELSE CAST(dat_encr_cc AS date)
        END as data_encerramento,
        -- Numeração das contas por cliente ordenado por data de abertura
        ROW_NUMBER() OVER (
            PARTITION BY cpf_raiz 
            ORDER BY dat_abet_cc
        ) as num_conta
    FROM base_contas
    WHERE dat_abet_cc IS NOT NULL
),

-- Análise de períodos com sobreposição
analise_periodos AS (
    SELECT 
        cpf_raiz,
        data_abertura,
        data_encerramento,
        -- Flag para início de novo período
        CASE 
            WHEN data_abertura > LAG(data_encerramento) OVER (
                PARTITION BY cpf_raiz 
                ORDER BY data_abertura
            ) OR num_conta = 1 THEN 1
            ELSE 0
        END as novo_periodo
    FROM base_normalizada
),

-- Consolidação dos períodos por cliente
periodos_consolidados AS (
    SELECT 
        cpf_raiz,
        MIN(data_abertura) as inicio_periodo,
        MAX(data_encerramento) as fim_periodo
    FROM (
        SELECT 
            cpf_raiz,
            data_abertura,
            data_encerramento,
            SUM(novo_periodo) OVER (
                PARTITION BY cpf_raiz 
                ORDER BY data_abertura
            ) as grupo_periodo
        FROM analise_periodos
    ) sub
    GROUP BY cpf_raiz, grupo_periodo
),

-- Cálculo dos tempos de relacionamento
calculo_tempos AS (
    SELECT 
        cpf_raiz,
        -- Tempo total desde primeira abertura
        DATEDIFF('month', 
            MIN(inicio_periodo), 
            MAX(fim_periodo)
        ) as tempo_total_meses,
        -- Tempo efetivo somando períodos sem sobreposição
        SUM(DATEDIFF('month', 
            inicio_periodo, 
            fim_periodo
        )) as tempo_efetivo_meses
    FROM periodos_consolidados
    GROUP BY cpf_raiz
),

-- Informações adicionais do cliente
info_cliente AS (
    SELECT 
        cpf_raiz,
        -- Segmento prioritário
        FIRST_VALUE(sgto) OVER (
            PARTITION BY cpf_raiz 
            ORDER BY sgto_ordem DESC, data_abertura
        ) as segmento_prioritario,
        -- Datas importantes
        MIN(data_abertura) as primeira_data_relacionamento,
        -- Contas ativas
        MIN(CASE 
            WHEN data_encerramento = CURRENT_DATE THEN data_abertura
        END) as data_conta_ativa_antiga,
        MAX(CASE 
            WHEN data_encerramento = CURRENT_DATE THEN data_abertura
        END) as data_conta_ativa_recente,
        -- Último encerramento
        MAX(CASE 
            WHEN data_encerramento != CURRENT_DATE THEN data_encerramento
        END) as data_ultimo_encerramento
    FROM base_normalizada
    GROUP BY cpf_raiz
)

-- Resultado final
SELECT 
    i.cpf_raiz,
    i.segmento_prioritario,
    i.primeira_data_relacionamento,
    i.data_conta_ativa_antiga,
    i.data_conta_ativa_recente,
    i.data_ultimo_encerramento,
    ct.tempo_total_meses,
    ct.tempo_efetivo_meses
FROM info_cliente i
JOIN calculo_tempos ct ON i.cpf_raiz = ct.cpf_raiz
ORDER BY i.cpf_raiz;


----------------------------------------------------------------------------------------------------------------------------

-- CHAT GPT V2
WITH base AS (
    SELECT
        cpf_raiz,
        agencia,
        conta,
        dac,
        sgto,
        COALESCE(NULLIF(dat_abet_cc, ''), '9999-12-31') AS dat_abet_cc,
        CASE 
            WHEN dat_encr_cc = '9999-12-31' OR dat_encr_cc IS NULL THEN current_date
            ELSE DATE(dat_encr_cc)
        END AS dat_encr_cc
    FROM
        sua_tabela
),

-- Passo 1: Encontrar o segmento prioritário por cliente
segmento_prioritario AS (
    SELECT
        cpf_raiz,
        MAX(CASE 
            WHEN sgto = '4' THEN 3
            WHEN sgto = 'L' THEN 2
            WHEN sgto = '3' THEN 1
            ELSE 0
        END) AS prioridade_segmento
    FROM base
    GROUP BY cpf_raiz
),

-- Passo 2: Obter datas e períodos de cada cliente
periodos AS (
    SELECT
        b.cpf_raiz,
        b.agencia,
        b.conta,
        b.dac,
        b.sgto,
        DATE(b.dat_abet_cc) AS dat_abet_cc,
        b.dat_encr_cc,
        ROW_NUMBER() OVER(PARTITION BY b.cpf_raiz ORDER BY DATE(b.dat_abet_cc)) AS ordem_abertura,
        ROW_NUMBER() OVER(PARTITION BY b.cpf_raiz ORDER BY DATE(b.dat_encr_cc) DESC) AS ordem_encerramento
    FROM base b
    LEFT JOIN segmento_prioritario sp ON b.cpf_raiz = sp.cpf_raiz
    WHERE sp.prioridade_segmento = 
        CASE 
            WHEN b.sgto = '4' THEN 3
            WHEN b.sgto = 'L' THEN 2
            WHEN b.sgto = '3' THEN 1
            ELSE 0
        END
),

-- Passo 3: Calcular a data da primeira abertura e do último encerramento para cada cliente
datas_extremas AS (
    SELECT 
        cpf_raiz,
        MIN(dat_abet_cc) AS primeira_abertura,  -- Primeira data de início de relacionamento
        MAX(CASE WHEN dat_encr_cc <> current_date THEN dat_encr_cc END) AS ultimo_encerramento  -- Último encerramento (contas que já foram encerradas)
    FROM periodos
    GROUP BY cpf_raiz
),

-- Passo 4: Obter as datas de abertura ativa mais antiga e mais recente
datas_ativas AS (
    SELECT 
        cpf_raiz,
        MIN(CASE WHEN dat_encr_cc = current_date THEN dat_abet_cc END) AS abertura_ativa_mais_antiga,
        MAX(CASE WHEN dat_encr_cc = current_date THEN dat_abet_cc END) AS abertura_ativa_mais_recente
    FROM periodos
    GROUP BY cpf_raiz
),

-- Passo 5: Calcular o tempo total e efetivo de relacionamento
tempo_relacionamento AS (
    SELECT
        p.cpf_raiz,
        de.primeira_abertura,
        de.ultimo_encerramento,
        da.abertura_ativa_mais_antiga,
        da.abertura_ativa_mais_recente,
        COUNT(DISTINCT p.dat_abet_cc) AS qtd_contas,
        DATEDIFF('month', de.primeira_abertura, COALESCE(de.ultimo_encerramento, current_date)) AS tempo_total_meses,
        SUM(
            DATEDIFF('month', LEAST(p.dat_encr_cc, LAG(p.dat_abet_cc, 1) OVER(PARTITION BY p.cpf_raiz ORDER BY p.dat_abet_cc)), p.dat_abet_cc)
        ) AS tempo_efetivo_meses
    FROM periodos p
    JOIN datas_extremas de ON p.cpf_raiz = de.cpf_raiz
    LEFT JOIN datas_ativas da ON p.cpf_raiz = da.cpf_raiz
    GROUP BY p.cpf_raiz, de.primeira_abertura, de.ultimo_encerramento, da.abertura_ativa_mais_antiga, da.abertura_ativa_mais_recente
)

-- Resultado final com o tempo de relacionamento total e efetivo
SELECT
    tr.cpf_raiz,
    tr.primeira_abertura, -- Primeira data de início de relacionamento
    tr.ultimo_encerramento, -- Data do último encerramento de conta
    tr.abertura_ativa_mais_antiga, -- Data de abertura da conta ativa mais antiga
    tr.abertura_ativa_mais_recente, -- Data de abertura da conta ativa mais recente
    tr.qtd_contas, -- Quantidade de contas ao longo do relacionamento
    tr.tempo_total_meses, -- Tempo total de relacionamento em meses
    tr.tempo_efetivo_meses -- Tempo efetivo de relacionamento em meses
FROM tempo_relacionamento tr
ORDER BY tr.cpf_raiz;

