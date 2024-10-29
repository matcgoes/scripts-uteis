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
