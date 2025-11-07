-- PASSO 1: Enriquecer as transações com informações societárias
WITH transacoes_enriquecidas AS (
    SELECT
        t.num_cpf_cnpj_emio,
        t.cod_tipo_pess_emio,
        t.num_cpf_cnpj_favo,
        t.cod_tipo_pess_favo,
        t.qtd_pix,
        t.vlr_pix,
        
        -- Identifica se emissor é empresa com sócios
        CASE WHEN qs_emio.num_cnpj_estb IS NOT NULL THEN 1 ELSE 0 END AS flag_emio_tem_socios,
        
        -- Identifica se emissor é sócio de alguma empresa
        CASE WHEN soc_emio.num_cpf_cnpj_scio IS NOT NULL THEN 1 ELSE 0 END AS flag_emio_eh_socio,
        
        -- Identifica se favorecido é empresa com sócios
        CASE WHEN qs_favo.num_cnpj_estb IS NOT NULL THEN 1 ELSE 0 END AS flag_favo_tem_socios,
        
        -- Identifica se favorecido é sócio de alguma empresa
        CASE WHEN soc_favo.num_cpf_cnpj_scio IS NOT NULL THEN 1 ELSE 0 END AS flag_favo_eh_socio,
        
        -- Mesma titularidade (mesmo CPF/CNPJ)
        CASE WHEN t.num_cpf_cnpj_emio = t.num_cpf_cnpj_favo THEN 1 ELSE 0 END AS flag_mesma_titularidade,
        
        -- Transação entre empresa e seu próprio sócio
        CASE WHEN rel_emp_soc.num_cnpj_estb IS NOT NULL THEN 1 ELSE 0 END AS flag_emp_para_proprio_socio,
        CASE WHEN rel_soc_emp.num_cnpj_estb IS NOT NULL THEN 1 ELSE 0 END AS flag_socio_para_propria_emp,
        
        -- Empresas do emissor (quando emissor é sócio)
        soc_emio.num_cnpj_estb AS cnpj_empresa_do_emio,
        
        -- Empresas do favorecido (quando favorecido é sócio)
        soc_favo.num_cnpj_estb AS cnpj_empresa_do_favo
        
    FROM base_transacoes t
    
    -- Verifica se EMISSOR é uma empresa com sócios cadastrados
    LEFT JOIN (
        SELECT DISTINCT num_cnpj_estb 
        FROM base_quadro_soc
    ) qs_emio ON t.num_cpf_cnpj_emio = qs_emio.num_cnpj_estb 
                 AND t.cod_tipo_pess_emio = 'J'
    
    -- Verifica se EMISSOR é sócio de alguma empresa
    LEFT JOIN base_quadro_soc soc_emio 
        ON t.num_cpf_cnpj_emio = soc_emio.num_cpf_cnpj_scio
        AND t.cod_tipo_pess_emio = soc_emio.cod_tipo_pess_scio
    
    -- Verifica se FAVORECIDO é uma empresa com sócios cadastrados
    LEFT JOIN (
        SELECT DISTINCT num_cnpj_estb 
        FROM base_quadro_soc
    ) qs_favo ON t.num_cpf_cnpj_favo = qs_favo.num_cnpj_estb 
                 AND t.cod_tipo_pess_favo = 'J'
    
    -- Verifica se FAVORECIDO é sócio de alguma empresa
    LEFT JOIN base_quadro_soc soc_favo 
        ON t.num_cpf_cnpj_favo = soc_favo.num_cpf_cnpj_scio
        AND t.cod_tipo_pess_favo = soc_favo.cod_tipo_pess_scio
    
    -- Relação direta: empresa pagando para seu próprio sócio
    LEFT JOIN base_quadro_soc rel_emp_soc
        ON t.num_cpf_cnpj_emio = rel_emp_soc.num_cnpj_estb
        AND t.num_cpf_cnpj_favo = rel_emp_soc.num_cpf_cnpj_scio
        AND t.cod_tipo_pess_favo = rel_emp_soc.cod_tipo_pess_scio
    
    -- Relação direta: sócio pagando para sua própria empresa
    LEFT JOIN base_quadro_soc rel_soc_emp
        ON t.num_cpf_cnpj_favo = rel_soc_emp.num_cnpj_estb
        AND t.num_cpf_cnpj_emio = rel_soc_emp.num_cpf_cnpj_scio
        AND t.cod_tipo_pess_emio = rel_soc_emp.cod_tipo_pess_scio
)

-- PASSO 2: Agregação por PAGADOR (Emissor)
, agg_pagador AS (
    SELECT
        num_cpf_cnpj_emio AS num_cpf_cnpj,
        cod_tipo_pess_emio AS cod_tipo_pess,
        'PAGADOR' AS tipo_posicao,
        
        -- Total geral
        COUNT(*) AS qtd_transacoes_pag,
        SUM(vlr_pix) AS vlr_total_pag,
        
        -- Mesma titularidade
        SUM(CASE WHEN flag_mesma_titularidade = 1 THEN vlr_pix ELSE 0 END) AS vlr_pag_mesma_titularidade,
        SUM(CASE WHEN flag_mesma_titularidade = 1 THEN 1 ELSE 0 END) AS qtd_pag_mesma_titularidade,
        
        -- Pagamentos para próprio sócio (se emissor é empresa)
        SUM(CASE WHEN flag_emp_para_proprio_socio = 1 THEN vlr_pix ELSE 0 END) AS vlr_pag_para_proprio_socio,
        SUM(CASE WHEN flag_emp_para_proprio_socio = 1 THEN 1 ELSE 0 END) AS qtd_pag_para_proprio_socio,
        
        -- Pagamentos para própria empresa (se emissor é sócio)
        SUM(CASE WHEN flag_socio_para_propria_emp = 1 THEN vlr_pix ELSE 0 END) AS vlr_pag_para_propria_empresa,
        SUM(CASE WHEN flag_socio_para_propria_emp = 1 THEN 1 ELSE 0 END) AS qtd_pag_para_propria_empresa,
        
        -- Pagamentos para PF
        SUM(CASE WHEN cod_tipo_pess_favo = 'F' THEN vlr_pix ELSE 0 END) AS vlr_pag_para_pf,
        SUM(CASE WHEN cod_tipo_pess_favo = 'F' THEN 1 ELSE 0 END) AS qtd_pag_para_pf,
        
        -- Pagamentos para PJ
        SUM(CASE WHEN cod_tipo_pess_favo = 'J' THEN vlr_pix ELSE 0 END) AS vlr_pag_para_pj,
        SUM(CASE WHEN cod_tipo_pess_favo = 'J' THEN 1 ELSE 0 END) AS qtd_pag_para_pj,
        
        -- Pagamentos onde favorecido é sócio de alguma empresa
        SUM(CASE WHEN flag_favo_eh_socio = 1 THEN vlr_pix ELSE 0 END) AS vlr_pag_para_socios,
        SUM(CASE WHEN flag_favo_eh_socio = 1 THEN 1 ELSE 0 END) AS qtd_pag_para_socios,
        
        -- Pagamentos onde favorecido é empresa com sócios
        SUM(CASE WHEN flag_favo_tem_socios = 1 THEN vlr_pix ELSE 0 END) AS vlr_pag_para_empresas,
        SUM(CASE WHEN flag_favo_tem_socios = 1 THEN 1 ELSE 0 END) AS qtd_pag_para_empresas
        
    FROM transacoes_enriquecidas
    GROUP BY num_cpf_cnpj_emio, cod_tipo_pess_emio
)

-- PASSO 3: Agregação por RECEBEDOR (Favorecido)
, agg_recebedor AS (
    SELECT
        num_cpf_cnpj_favo AS num_cpf_cnpj,
        cod_tipo_pess_favo AS cod_tipo_pess,
        'RECEBEDOR' AS tipo_posicao,
        
        -- Total geral
        COUNT(*) AS qtd_transacoes_rec,
        SUM(vlr_pix) AS vlr_total_rec,
        
        -- Mesma titularidade
        SUM(CASE WHEN flag_mesma_titularidade = 1 THEN vlr_pix ELSE 0 END) AS vlr_rec_mesma_titularidade,
        SUM(CASE WHEN flag_mesma_titularidade = 1 THEN 1 ELSE 0 END) AS qtd_rec_mesma_titularidade,
        
        -- Recebimentos de próprio sócio (se favorecido é empresa)
        SUM(CASE WHEN flag_socio_para_propria_emp = 1 THEN vlr_pix ELSE 0 END) AS vlr_rec_de_proprio_socio,
        SUM(CASE WHEN flag_socio_para_propria_emp = 1 THEN 1 ELSE 0 END) AS qtd_rec_de_proprio_socio,
        
        -- Recebimentos de própria empresa (se favorecido é sócio)
        SUM(CASE WHEN flag_emp_para_proprio_socio = 1 THEN vlr_pix ELSE 0 END) AS vlr_rec_de_propria_empresa,
        SUM(CASE WHEN flag_emp_para_proprio_socio = 1 THEN 1 ELSE 0 END) AS qtd_rec_de_propria_empresa,
        
        -- Recebimentos de PF
        SUM(CASE WHEN cod_tipo_pess_emio = 'F' THEN vlr_pix ELSE 0 END) AS vlr_rec_de_pf,
        SUM(CASE WHEN cod_tipo_pess_emio = 'F' THEN 1 ELSE 0 END) AS qtd_rec_de_pf,
        
        -- Recebimentos de PJ
        SUM(CASE WHEN cod_tipo_pess_emio = 'J' THEN vlr_pix ELSE 0 END) AS vlr_rec_de_pj,
        SUM(CASE WHEN cod_tipo_pess_emio = 'J' THEN 1 ELSE 0 END) AS qtd_rec_de_pj,
        
        -- Recebimentos onde emissor é sócio de alguma empresa
        SUM(CASE WHEN flag_emio_eh_socio = 1 THEN vlr_pix ELSE 0 END) AS vlr_rec_de_socios,
        SUM(CASE WHEN flag_emio_eh_socio = 1 THEN 1 ELSE 0 END) AS qtd_rec_de_socios,
        
        -- Recebimentos onde emissor é empresa com sócios
        SUM(CASE WHEN flag_emio_tem_socios = 1 THEN vlr_pix ELSE 0 END) AS vlr_rec_de_empresas,
        SUM(CASE WHEN flag_emio_tem_socios = 1 THEN 1 ELSE 0 END) AS qtd_rec_de_empresas
        
    FROM transacoes_enriquecidas
    GROUP BY num_cpf_cnpj_favo, cod_tipo_pess_favo
)

-- PASSO 4: Consolidação Final
SELECT
    COALESCE(p.num_cpf_cnpj, r.num_cpf_cnpj) AS num_cpf_cnpj,
    COALESCE(p.cod_tipo_pess, r.cod_tipo_pess) AS cod_tipo_pess,
    
    -- Métricas de Pagamento
    COALESCE(p.qtd_transacoes_pag, 0) AS qtd_transacoes_pag,
    COALESCE(p.vlr_total_pag, 0) AS vlr_total_pag,
    COALESCE(p.vlr_pag_mesma_titularidade, 0) AS vlr_pag_mesma_titularidade,
    COALESCE(p.qtd_pag_mesma_titularidade, 0) AS qtd_pag_mesma_titularidade,
    COALESCE(p.vlr_pag_para_proprio_socio, 0) AS vlr_pag_para_proprio_socio,
    COALESCE(p.qtd_pag_para_proprio_socio, 0) AS qtd_pag_para_proprio_socio,
    COALESCE(p.vlr_pag_para_propria_empresa, 0) AS vlr_pag_para_propria_empresa,
    COALESCE(p.qtd_pag_para_propria_empresa, 0) AS qtd_pag_para_propria_empresa,
    COALESCE(p.vlr_pag_para_pf, 0) AS vlr_pag_para_pf,
    COALESCE(p.qtd_pag_para_pf, 0) AS qtd_pag_para_pf,
    COALESCE(p.vlr_pag_para_pj, 0) AS vlr_pag_para_pj,
    COALESCE(p.qtd_pag_para_pj, 0) AS qtd_pag_para_pj,
    COALESCE(p.vlr_pag_para_socios, 0) AS vlr_pag_para_socios,
    COALESCE(p.qtd_pag_para_socios, 0) AS qtd_pag_para_socios,
    COALESCE(p.vlr_pag_para_empresas, 0) AS vlr_pag_para_empresas,
    COALESCE(p.qtd_pag_para_empresas, 0) AS qtd_pag_para_empresas,
    
    -- Métricas de Recebimento
    COALESCE(r.qtd_transacoes_rec, 0) AS qtd_transacoes_rec,
    COALESCE(r.vlr_total_rec, 0) AS vlr_total_rec,
    COALESCE(r.vlr_rec_mesma_titularidade, 0) AS vlr_rec_mesma_titularidade,
    COALESCE(r.qtd_rec_mesma_titularidade, 0) AS qtd_rec_mesma_titularidade,
    COALESCE(r.vlr_rec_de_proprio_socio, 0) AS vlr_rec_de_proprio_socio,
    COALESCE(r.qtd_rec_de_proprio_socio, 0) AS qtd_rec_de_proprio_socio,
    COALESCE(r.vlr_rec_de_propria_empresa, 0) AS vlr_rec_de_propria_empresa,
    COALESCE(r.qtd_rec_de_propria_empresa, 0) AS qtd_rec_de_propria_empresa,
    COALESCE(r.vlr_rec_de_pf, 0) AS vlr_rec_de_pf,
    COALESCE(r.qtd_rec_de_pf, 0) AS qtd_rec_de_pf,
    COALESCE(r.vlr_rec_de_pj, 0) AS vlr_rec_de_pj,
    COALESCE(r.qtd_rec_de_pj, 0) AS qtd_rec_de_pj,
    COALESCE(r.vlr_rec_de_socios, 0) AS vlr_rec_de_socios,
    COALESCE(r.qtd_rec_de_socios, 0) AS qtd_rec_de_socios,
    COALESCE(r.vlr_rec_de_empresas, 0) AS vlr_rec_de_empresas,
    COALESCE(r.qtd_rec_de_empresas, 0) AS qtd_rec_de_empresas
    
FROM agg_pagador p
FULL OUTER JOIN agg_recebedor r
    ON p.num_cpf_cnpj = r.num_cpf_cnpj
    AND p.cod_tipo_pess = r.cod_tipo_pess;

-- ========================================================================


-- ========================================================================
-- QUERY CONSOLIDADA: Análise de Transações PIX com Vínculos Societários
-- ========================================================================

WITH transacoes_enriquecidas AS (
    SELECT
        t.num_cpf_cnpj_emio,
        t.cod_tipo_pess_emio,
        t.num_cpf_cnpj_favo,
        t.cod_tipo_pess_favo,
        t.qtd_pix,
        t.vlr_pix,
        
        -- FLAGS DE IDENTIFICAÇÃO
        
        -- Mesma titularidade (mesmo CPF/CNPJ)
        CASE WHEN t.num_cpf_cnpj_emio = t.num_cpf_cnpj_favo THEN 1 ELSE 0 END AS flag_mesma_titularidade,
        
        -- Transação entre empresa e seu próprio sócio (Empresa → Sócio)
        CASE WHEN rel_emp_soc.num_cnpj_estb IS NOT NULL THEN 1 ELSE 0 END AS flag_emp_para_proprio_socio,
        
        -- Transação entre sócio e sua própria empresa (Sócio → Empresa)
        CASE WHEN rel_soc_emp.num_cnpj_estb IS NOT NULL THEN 1 ELSE 0 END AS flag_socio_para_propria_emp,
        
        -- Emissor é sócio de alguma empresa
        CASE WHEN soc_emio.num_cpf_cnpj_scio IS NOT NULL THEN 1 ELSE 0 END AS flag_emio_eh_socio,
        
        -- Emissor é empresa com sócios cadastrados
        CASE WHEN emp_emio.num_cnpj_estb IS NOT NULL THEN 1 ELSE 0 END AS flag_emio_tem_socios,
        
        -- Favorecido é sócio de alguma empresa
        CASE WHEN soc_favo.num_cpf_cnpj_scio IS NOT NULL THEN 1 ELSE 0 END AS flag_favo_eh_socio,
        
        -- Favorecido é empresa com sócios cadastrados
        CASE WHEN emp_favo.num_cnpj_estb IS NOT NULL THEN 1 ELSE 0 END AS flag_favo_tem_socios,
        
        -- TRANSAÇÕES COM EMPRESAS DOS SÓCIOS (emissor é sócio pagando para empresa de outro sócio)
        CASE WHEN emp_outros_socios_emio.num_cnpj_estb IS NOT NULL THEN 1 ELSE 0 END AS flag_pag_empresa_outros_socios,
        
        -- TRANSAÇÕES COM EMPRESAS DOS SÓCIOS (favorecido é sócio recebendo de empresa de outro sócio)
        CASE WHEN emp_outros_socios_favo.num_cnpj_estb IS NOT NULL THEN 1 ELSE 0 END AS flag_rec_empresa_outros_socios
        
    FROM base_transacoes t
    
    -- Relação direta: empresa pagando para seu próprio sócio
    LEFT JOIN base_quadro_soc rel_emp_soc
        ON t.num_cpf_cnpj_emio = rel_emp_soc.num_cnpj_estb
        AND t.cod_tipo_pess_emio = 'J'
        AND t.num_cpf_cnpj_favo = rel_emp_soc.num_cpf_cnpj_scio
        AND t.cod_tipo_pess_favo = rel_emp_soc.cod_tipo_pess_scio
    
    -- Relação direta: sócio pagando para sua própria empresa
    LEFT JOIN base_quadro_soc rel_soc_emp
        ON t.num_cpf_cnpj_favo = rel_soc_emp.num_cnpj_estb
        AND t.cod_tipo_pess_favo = 'J'
        AND t.num_cpf_cnpj_emio = rel_soc_emp.num_cpf_cnpj_scio
        AND t.cod_tipo_pess_emio = rel_soc_emp.cod_tipo_pess_scio
    
    -- Verifica se EMISSOR é sócio de alguma empresa
    LEFT JOIN (
        SELECT DISTINCT num_cpf_cnpj_scio, cod_tipo_pess_scio
        FROM base_quadro_soc
    ) soc_emio 
        ON t.num_cpf_cnpj_emio = soc_emio.num_cpf_cnpj_scio
        AND t.cod_tipo_pess_emio = soc_emio.cod_tipo_pess_scio
    
    -- Verifica se EMISSOR é empresa com sócios cadastrados
    LEFT JOIN (
        SELECT DISTINCT num_cnpj_estb
        FROM base_quadro_soc
    ) emp_emio 
        ON t.num_cpf_cnpj_emio = emp_emio.num_cnpj_estb
        AND t.cod_tipo_pess_emio = 'J'
    
    -- Verifica se FAVORECIDO é sócio de alguma empresa
    LEFT JOIN (
        SELECT DISTINCT num_cpf_cnpj_scio, cod_tipo_pess_scio
        FROM base_quadro_soc
    ) soc_favo 
        ON t.num_cpf_cnpj_favo = soc_favo.num_cpf_cnpj_scio
        AND t.cod_tipo_pess_favo = soc_favo.cod_tipo_pess_scio
    
    -- Verifica se FAVORECIDO é empresa com sócios cadastrados
    LEFT JOIN (
        SELECT DISTINCT num_cnpj_estb
        FROM base_quadro_soc
    ) emp_favo 
        ON t.num_cpf_cnpj_favo = emp_favo.num_cnpj_estb
        AND t.cod_tipo_pess_favo = 'J'
    
    -- EMISSOR é sócio e FAVORECIDO é empresa de OUTRO sócio (não a própria)
    LEFT JOIN base_quadro_soc emp_outros_socios_emio
        ON t.num_cpf_cnpj_favo = emp_outros_socios_emio.num_cnpj_estb
        AND t.cod_tipo_pess_favo = 'J'
        AND EXISTS (
            SELECT 1 FROM base_quadro_soc qs
            WHERE qs.num_cpf_cnpj_scio = t.num_cpf_cnpj_emio
            AND qs.cod_tipo_pess_scio = t.cod_tipo_pess_emio
            AND qs.num_cnpj_estb != t.num_cpf_cnpj_favo  -- Empresa diferente da que está pagando
        )
    
    -- FAVORECIDO é sócio e EMISSOR é empresa de OUTRO sócio (não a própria)
    LEFT JOIN base_quadro_soc emp_outros_socios_favo
        ON t.num_cpf_cnpj_emio = emp_outros_socios_favo.num_cnpj_estb
        AND t.cod_tipo_pess_emio = 'J'
        AND EXISTS (
            SELECT 1 FROM base_quadro_soc qs
            WHERE qs.num_cpf_cnpj_scio = t.num_cpf_cnpj_favo
            AND qs.cod_tipo_pess_scio = t.cod_tipo_pess_favo
            AND qs.num_cnpj_estb != t.num_cpf_cnpj_emio  -- Empresa diferente da que está recebendo
        )
)

-- ========================================================================
-- AGREGAÇÃO POR PAGADOR (Emissor)
-- ========================================================================
, agg_pagador AS (
    SELECT
        num_cpf_cnpj_emio AS num_cpf_cnpj,
        cod_tipo_pess_emio AS cod_tipo_pess,
        
        -- TOTAIS GERAIS
        COUNT(*) AS qtd_transacoes_pag,
        SUM(vlr_pix) AS vlr_total_pag,
        
        -- 1. MESMA TITULARIDADE
        SUM(CASE WHEN flag_mesma_titularidade = 1 THEN vlr_pix ELSE 0 END) AS vlr_pag_mesma_titularidade,
        SUM(CASE WHEN flag_mesma_titularidade = 1 THEN 1 ELSE 0 END) AS qtd_pag_mesma_titularidade,
        
        -- 2. PAGAMENTOS POR SÓCIOS (quando emissor é empresa pagando ao próprio sócio)
        SUM(CASE WHEN flag_emp_para_proprio_socio = 1 THEN vlr_pix ELSE 0 END) AS vlr_pag_para_proprio_socio,
        SUM(CASE WHEN flag_emp_para_proprio_socio = 1 THEN 1 ELSE 0 END) AS qtd_pag_para_proprio_socio,
        
        -- 3. PAGAMENTOS PELAS EMPRESAS (quando emissor é sócio pagando à própria empresa)
        SUM(CASE WHEN flag_socio_para_propria_emp = 1 THEN vlr_pix ELSE 0 END) AS vlr_pag_para_propria_empresa,
        SUM(CASE WHEN flag_socio_para_propria_emp = 1 THEN 1 ELSE 0 END) AS qtd_pag_para_propria_empresa,
        
        -- 4. PAGAMENTOS DE PESSOA FÍSICA OU JURÍDICA
        SUM(CASE WHEN cod_tipo_pess_favo = 'F' THEN vlr_pix ELSE 0 END) AS vlr_pag_para_pf,
        SUM(CASE WHEN cod_tipo_pess_favo = 'F' THEN 1 ELSE 0 END) AS qtd_pag_para_pf,
        
        SUM(CASE WHEN cod_tipo_pess_favo = 'J' THEN vlr_pix ELSE 0 END) AS vlr_pag_para_pj,
        SUM(CASE WHEN cod_tipo_pess_favo = 'J' THEN 1 ELSE 0 END) AS qtd_pag_para_pj,
        
        -- COMPLEMENTARES: Pagamentos onde favorecido é sócio ou empresa
        SUM(CASE WHEN flag_favo_eh_socio = 1 THEN vlr_pix ELSE 0 END) AS vlr_pag_para_quem_eh_socio,
        SUM(CASE WHEN flag_favo_eh_socio = 1 THEN 1 ELSE 0 END) AS qtd_pag_para_quem_eh_socio,
        
        SUM(CASE WHEN flag_favo_tem_socios = 1 THEN vlr_pix ELSE 0 END) AS vlr_pag_para_empresas_com_socios,
        SUM(CASE WHEN flag_favo_tem_socios = 1 THEN 1 ELSE 0 END) AS qtd_pag_para_empresas_com_socios,
        
        -- 5. PAGAMENTOS REFERENTES A EMPRESAS DOS SÓCIOS ATRELADOS AO CNPJ
        SUM(CASE WHEN flag_pag_empresa_outros_socios = 1 THEN vlr_pix ELSE 0 END) AS vlr_pag_empresas_outros_socios,
        SUM(CASE WHEN flag_pag_empresa_outros_socios = 1 THEN 1 ELSE 0 END) AS qtd_pag_empresas_outros_socios
        
    FROM transacoes_enriquecidas
    GROUP BY num_cpf_cnpj_emio, cod_tipo_pess_emio
)

-- ========================================================================
-- AGREGAÇÃO POR RECEBEDOR (Favorecido)
-- ========================================================================
, agg_recebedor AS (
    SELECT
        num_cpf_cnpj_favo AS num_cpf_cnpj,
        cod_tipo_pess_favo AS cod_tipo_pess,
        
        -- TOTAIS GERAIS
        COUNT(*) AS qtd_transacoes_rec,
        SUM(vlr_pix) AS vlr_total_rec,
        
        -- 1. MESMA TITULARIDADE
        SUM(CASE WHEN flag_mesma_titularidade = 1 THEN vlr_pix ELSE 0 END) AS vlr_rec_mesma_titularidade,
        SUM(CASE WHEN flag_mesma_titularidade = 1 THEN 1 ELSE 0 END) AS qtd_rec_mesma_titularidade,
        
        -- 2. RECEBIMENTOS POR SÓCIOS (quando favorecido é empresa recebendo do próprio sócio)
        SUM(CASE WHEN flag_socio_para_propria_emp = 1 THEN vlr_pix ELSE 0 END) AS vlr_rec_de_proprio_socio,
        SUM(CASE WHEN flag_socio_para_propria_emp = 1 THEN 1 ELSE 0 END) AS qtd_rec_de_proprio_socio,
        
        -- 3. RECEBIMENTOS PELAS EMPRESAS (quando favorecido é sócio recebendo da própria empresa)
        SUM(CASE WHEN flag_emp_para_proprio_socio = 1 THEN vlr_pix ELSE 0 END) AS vlr_rec_de_propria_empresa,
        SUM(CASE WHEN flag_emp_para_proprio_socio = 1 THEN 1 ELSE 0 END) AS qtd_rec_de_propria_empresa,
        
        -- 4. RECEBIMENTOS DE PESSOA FÍSICA OU JURÍDICA
        SUM(CASE WHEN cod_tipo_pess_emio = 'F' THEN vlr_pix ELSE 0 END) AS vlr_rec_de_pf,
        SUM(CASE WHEN cod_tipo_pess_emio = 'F' THEN 1 ELSE 0 END) AS qtd_rec_de_pf,
        
        SUM(CASE WHEN cod_tipo_pess_emio = 'J' THEN vlr_pix ELSE 0 END) AS vlr_rec_de_pj,
        SUM(CASE WHEN cod_tipo_pess_emio = 'J' THEN 1 ELSE 0 END) AS qtd_rec_de_pj,
        
        -- COMPLEMENTARES: Recebimentos onde emissor é sócio ou empresa
        SUM(CASE WHEN flag_emio_eh_socio = 1 THEN vlr_pix ELSE 0 END) AS vlr_rec_de_quem_eh_socio,
        SUM(CASE WHEN flag_emio_eh_socio = 1 THEN 1 ELSE 0 END) AS qtd_rec_de_quem_eh_socio,
        
        SUM(CASE WHEN flag_emio_tem_socios = 1 THEN vlr_pix ELSE 0 END) AS vlr_rec_de_empresas_com_socios,
        SUM(CASE WHEN flag_emio_tem_socios = 1 THEN 1 ELSE 0 END) AS qtd_rec_de_empresas_com_socios,
        
        -- 5. RECEBIMENTOS REFERENTES A EMPRESAS DOS SÓCIOS ATRELADOS AO CNPJ
        SUM(CASE WHEN flag_rec_empresa_outros_socios = 1 THEN vlr_pix ELSE 0 END) AS vlr_rec_empresas_outros_socios,
        SUM(CASE WHEN flag_rec_empresa_outros_socios = 1 THEN 1 ELSE 0 END) AS qtd_rec_empresas_outros_socios
        
    FROM transacoes_enriquecidas
    GROUP BY num_cpf_cnpj_favo, cod_tipo_pess_favo
)

-- ========================================================================
-- CONSOLIDAÇÃO FINAL
-- ========================================================================
SELECT
    COALESCE(p.num_cpf_cnpj, r.num_cpf_cnpj) AS num_cpf_cnpj,
    COALESCE(p.cod_tipo_pess, r.cod_tipo_pess) AS cod_tipo_pess,
    
    -- ============== MÉTRICAS DE PAGAMENTO ==============
    COALESCE(p.qtd_transacoes_pag, 0) AS qtd_transacoes_pag,
    COALESCE(p.vlr_total_pag, 0) AS vlr_total_pag,
    
    -- Mesma titularidade
    COALESCE(p.vlr_pag_mesma_titularidade, 0) AS vlr_pag_mesma_titularidade,
    COALESCE(p.qtd_pag_mesma_titularidade, 0) AS qtd_pag_mesma_titularidade,
    
    -- Pagamentos por sócios
    COALESCE(p.vlr_pag_para_proprio_socio, 0) AS vlr_pag_para_proprio_socio,
    COALESCE(p.qtd_pag_para_proprio_socio, 0) AS qtd_pag_para_proprio_socio,
    
    -- Pagamentos pelas empresas
    COALESCE(p.vlr_pag_para_propria_empresa, 0) AS vlr_pag_para_propria_empresa,
    COALESCE(p.qtd_pag_para_propria_empresa, 0) AS qtd_pag_para_propria_empresa,
    
    -- Pagamentos para PF/PJ
    COALESCE(p.vlr_pag_para_pf, 0) AS vlr_pag_para_pf,
    COALESCE(p.qtd_pag_para_pf, 0) AS qtd_pag_para_pf,
    COALESCE(p.vlr_pag_para_pj, 0) AS vlr_pag_para_pj,
    COALESCE(p.qtd_pag_para_pj, 0) AS qtd_pag_para_pj,
    
    -- Complementares de pagamento
    COALESCE(p.vlr_pag_para_quem_eh_socio, 0) AS vlr_pag_para_quem_eh_socio,
    COALESCE(p.qtd_pag_para_quem_eh_socio, 0) AS qtd_pag_para_quem_eh_socio,
    COALESCE(p.vlr_pag_para_empresas_com_socios, 0) AS vlr_pag_para_empresas_com_socios,
    COALESCE(p.qtd_pag_para_empresas_com_socios, 0) AS qtd_pag_para_empresas_com_socios,
    
    -- Pagamentos para empresas de outros sócios
    COALESCE(p.vlr_pag_empresas_outros_socios, 0) AS vlr_pag_empresas_outros_socios,
    COALESCE(p.qtd_pag_empresas_outros_socios, 0) AS qtd_pag_empresas_outros_socios,
    
    -- ============== MÉTRICAS DE RECEBIMENTO ==============
    COALESCE(r.qtd_transacoes_rec, 0) AS qtd_transacoes_rec,
    COALESCE(r.vlr_total_rec, 0) AS vlr_total_rec,
    
    -- Mesma titularidade
    COALESCE(r.vlr_rec_mesma_titularidade, 0) AS vlr_rec_mesma_titularidade,
    COALESCE(r.qtd_rec_mesma_titularidade, 0) AS qtd_rec_mesma_titularidade,
    
    -- Recebimentos por sócios
    COALESCE(r.vlr_rec_de_proprio_socio, 0) AS vlr_rec_de_proprio_socio,
    COALESCE(r.qtd_rec_de_proprio_socio, 0) AS qtd_rec_de_proprio_socio,
    
    -- Recebimentos pelas empresas
    COALESCE(r.vlr_rec_de_propria_empresa, 0) AS vlr_rec_de_propria_empresa,
    COALESCE(r.qtd_rec_de_propria_empresa, 0) AS qtd_rec_de_propria_empresa,
    
    -- Recebimentos de PF/PJ
    COALESCE(r.vlr_rec_de_pf, 0) AS vlr_rec_de_pf,
    COALESCE(r.qtd_rec_de_pf, 0) AS qtd_rec_de_pf,
    COALESCE(r.vlr_rec_de_pj, 0) AS vlr_rec_de_pj,
    COALESCE(r.qtd_rec_de_pj, 0) AS qtd_rec_de_pj,
    
    -- Complementares de recebimento
    COALESCE(r.vlr_rec_de_quem_eh_socio, 0) AS vlr_rec_de_quem_eh_socio,
    COALESCE(r.qtd_rec_de_quem_eh_socio, 0) AS qtd_rec_de_quem_eh_socio,
    COALESCE(r.vlr_rec_de_empresas_com_socios, 0) AS vlr_rec_de_empresas_com_socios,
    COALESCE(r.qtd_rec_de_empresas_com_socios, 0) AS qtd_rec_de_empresas_com_socios,
    
    -- Recebimentos de empresas de outros sócios
    COALESCE(r.vlr_rec_empresas_outros_socios, 0) AS vlr_rec_empresas_outros_socios,
    COALESCE(r.qtd_rec_empresas_outros_socios, 0) AS qtd_rec_empresas_outros_socios
    
FROM agg_pagador p
FULL OUTER JOIN agg_recebedor r
    ON p.num_cpf_cnpj = r.num_cpf_cnpj
    AND p.cod_tipo_pess = r.cod_tipo_pess;


-- ====================================
-- VERSAO GPT!
-- ====================================

WITH tx AS (
  SELECT
    REGEXP_REPLACE(num_cpf_cnpj_emio, '[^0-9]', '') AS em_doc,
    UPPER(cod_tipo_pess_emio)                        AS em_tp,   -- 'J' ou 'F'
    REGEXP_REPLACE(num_cpf_cnpj_favo, '[^0-9]', '') AS fv_doc,
    UPPER(cod_tipo_pess_favo)                        AS fv_tp,   -- 'J' ou 'F'
    qtd_pix,
    vlr_pix
    -- , dt_transacao
  FROM base_transacoes
),
soc AS (
  SELECT DISTINCT
    REGEXP_REPLACE(num_cnpj_estb,     '[^0-9]', '') AS emp_doc,
    UPPER(cod_tipo_pess)                           AS emp_tp,   -- em geral 'J'
    REGEXP_REPLACE(num_cpf_cnpj_scio, '[^0-9]', '') AS soc_doc,
    UPPER(cod_tipo_pess_scio)                      AS soc_tp    -- 'F' ou 'J'
  FROM base_quadro_soc
  -- WHERE dt_ini <= dt_transacao AND (dt_fim IS NULL OR dt_fim >= dt_transacao)
),

-- pares (empresa A, empresa B) que compartilham ao menos 1 mesmo sócio
emp_emp_mesmo_socio AS (
  SELECT DISTINCT
    s1.emp_doc AS emp_a,
    s2.emp_doc AS emp_b
  FROM soc s1
  JOIN soc s2
    ON s1.soc_doc = s2.soc_doc
   AND s1.soc_tp  = s2.soc_tp
   AND s1.emp_doc <> s2.emp_doc
),

enriched AS (
  SELECT
    t.*,

    -- 1) mesma titularidade? (mesmo doc+tipo nas duas pontas)
    CASE WHEN t.em_doc = t.fv_doc AND t.em_tp = t.fv_tp THEN 1 ELSE 0 END AS fl_mesma_titularidade,

    -- 2) relação empresa->sócio direta
    CASE WHEN EXISTS (
      SELECT 1 FROM soc s
      WHERE t.em_doc = s.emp_doc AND t.em_tp = s.emp_tp
        AND t.fv_doc = s.soc_doc AND t.fv_tp = s.soc_tp
    ) THEN 1 ELSE 0 END AS fl_emp_para_soc,

    -- 3) relação sócio->empresa direta
    CASE WHEN EXISTS (
      SELECT 1 FROM soc s
      WHERE t.em_doc = s.soc_doc AND t.em_tp = s.soc_tp
        AND t.fv_doc = s.emp_doc AND t.fv_tp = s.emp_tp
    ) THEN 1 ELSE 0 END AS fl_soc_para_emp,

    -- 4) emissor é empresa? recebedor é empresa? (PF/PJ)
    CASE WHEN t.em_tp = 'J' THEN 1 ELSE 0 END AS fl_emissor_empresa,
    CASE WHEN t.em_tp = 'F' THEN 1 ELSE 0 END AS fl_emissor_pf,
    CASE WHEN t.fv_tp = 'J' THEN 1 ELSE 0 END AS fl_recebedor_empresa,
    CASE WHEN t.fv_tp = 'F' THEN 1 ELSE 0 END AS fl_recebedor_pf,

    -- 5) “referente às empresa(s) do(s) sócio(s) atrelado(s) ao CNPJ”
    --    (dois saltos): se EMISSOR é CNPJ A e FAVORECIDO é CNPJ B e A↔B compartilham sócio
    CASE WHEN t.em_tp='J' AND t.fv_tp='J' AND EXISTS (
      SELECT 1 FROM emp_emp_mesmo_socio m
      WHERE m.emp_a = t.em_doc AND m.emp_b = t.fv_doc
    ) THEN 1 ELSE 0 END AS fl_emissor_para_empresas_dos_meus_socios,

    -- idem, do lado de quem RECEBE (B recebe de A)
    CASE WHEN t.em_tp='J' AND t.fv_tp='J' AND EXISTS (
      SELECT 1 FROM emp_emp_mesmo_socio m
      WHERE m.emp_b = t.fv_doc AND m.emp_a = t.em_doc
    ) THEN 1 ELSE 0 END AS fl_recebedor_de_empresas_dos_meus_socios
  FROM tx t
)
SELECT * FROM enriched;

-- ***************

-- =============================================================
-- ADICIONANDO ATUALIZAÇÕES DE EXEMPLO
-- =============================================================

WITH tx AS (
  SELECT
    REGEXP_REPLACE(num_cpf_cnpj_emio, '[^0-9]', '') AS em_doc,
    UPPER(cod_tipo_pess_emio)                        AS em_tp,
    REGEXP_REPLACE(num_cpf_cnpj_favo, '[^0-9]', '') AS fv_doc,
    UPPER(cod_tipo_pess_favo)                        AS fv_tp,
    CAST(vlr_pix AS DOUBLE)                          AS vlr_pix
  FROM base_transacoes
  -- ${WHERE_CLAUSE_TX}
),
soc AS (
  SELECT DISTINCT
    REGEXP_REPLACE(num_cnpj_estb,     '[^0-9]', '') AS emp_doc,   -- PJ focal
    UPPER(cod_tipo_pess)                            AS emp_tp,    -- tipicamente 'J'
    REGEXP_REPLACE(num_cpf_cnpj_scio, '[^0-9]', '') AS soc_doc,   -- PF/PJ sócio
    UPPER(cod_tipo_pess_scio)                       AS soc_tp
  FROM base_quadro_soc
  -- ${WHERE_CLAUSE_SOC}
),

-- pré-aggregates por participante (doc+tp)
rec_ AS (
  SELECT fv_doc AS doc, fv_tp AS tp,
         SUM(vlr_pix) AS vlr_recebido, COUNT(*) AS qtd_recebida
  FROM tx
  GROUP BY fv_doc, fv_tp
),
pag_ AS (
  SELECT em_doc AS doc, em_tp AS tp,
         SUM(vlr_pix) AS vlr_pago, COUNT(*) AS qtd_paga
  FROM tx
  GROUP BY em_doc, em_tp
),

-- Nível 1: Global dos sócios reatribuído à empresa
socio_global_por_empresa AS (
  SELECT
    s.emp_doc,
    s.emp_tp,
    COALESCE(SUM(r.vlr_recebido), 0) AS vlr_recm_totl_scio,
    COALESCE(SUM(r.qtd_recebida), 0) AS qtd_recm_totl_scio,
    COALESCE(SUM(p.vlr_pago),     0) AS vlr_pgto_totl_scio,
    COALESCE(SUM(p.qtd_paga),     0) AS qtd_pgto_totl_scio
  FROM soc s
  LEFT JOIN rec_ r
    ON r.doc = s.soc_doc AND r.tp = s.soc_tp
  LEFT JOIN pag_ p
    ON p.doc = s.soc_doc AND p.tp = s.soc_tp
  GROUP BY s.emp_doc, s.emp_tp
),

-- pares de empresas com sócio em comum
emp_emp_mesmo_socio AS (
  SELECT DISTINCT
    s1.emp_doc AS emp_a,   -- empresa focal
    s2.emp_doc AS emp_b    -- outra empresa dos seus sócios
  FROM soc s1
  JOIN soc s2
    ON s1.soc_doc = s2.soc_doc
   AND s1.soc_tp  = s2.soc_tp
   AND s1.emp_doc <> s2.emp_doc
),

-- Nível 2: Reatribuição do que OUTRAS empresas (emp_b) receberam/pagaram
empresas_dos_socios_por_empresa AS (
  SELECT
    m.emp_a AS emp_doc,
    COALESCE(SUM(r.vlr_recebido), 0) AS vlr_recm_totl_empr_asdo,
    COALESCE(SUM(r.qtd_recebida), 0) AS qtd_recm_totl_empr_asdo,
    COALESCE(SUM(p.vlr_pago),     0) AS vlr_pgto_empr_asdo,
    COALESCE(SUM(p.qtd_paga),     0) AS qtd_pgto_empr_asdo
  FROM emp_emp_mesmo_socio m
  LEFT JOIN rec_ r ON r.doc = m.emp_b AND r.tp = 'J'   -- só empresas
  LEFT JOIN pag_ p ON p.doc = m.emp_b AND p.tp = 'J'
  GROUP BY m.emp_a
)

-- SAÍDA ÚNICA
SELECT
  COALESCE(p.doc, r.doc) AS doc,                -- pode ser PF ou PJ
  COALESCE(p.tp,  r.tp)  AS tp,
  COALESCE(p.vlr_pago,     0) AS vlr_pago,
  COALESCE(p.qtd_paga,     0) AS qtd_paga,
  COALESCE(r.vlr_recebido, 0) AS vlr_recebido,
  COALESCE(r.qtd_recebida, 0) AS qtd_recebida,
  COALESCE(nv1.vlr_recm_totl_scio, 0) AS vlr_recm_totl_scio,
  COALESCE(nv1.qtd_recm_totl_scio, 0) AS qtd_recm_totl_scio,
  COALESCE(nv1.vlr_pgto_totl_scio, 0) AS vlr_pgto_totl_scio,
  COALESCE(nv1.qtd_pgto_totl_scio, 0) AS qtd_pgto_totl_scio,
  COALESCE(nv2.vlr_recm_totl_empr_asdo, 0) AS vlr_recm_totl_empr_asdo,
  COALESCE(nv2.qtd_recm_totl_empr_asdo, 0) AS qtd_recm_totl_empr_asdo,
  COALESCE(nv2.vlr_pgto_empr_asdo,      0) AS vlr_pgto_empr_asdo,
  COALESCE(nv2.qtd_pgto_empr_asdo,      0) AS qtd_pgto_empr_asdo
FROM pag_ p
FULL JOIN rec_ r
  ON p.doc = r.doc AND p.tp = r.tp
LEFT JOIN socio_global_por_empresa        nv1
  ON COALESCE(p.doc, r.doc) = nv1.emp_doc
 AND COALESCE(p.tp,  r.tp)  = nv1.emp_tp
LEFT JOIN empresas_dos_socios_por_empresa nv2
  ON COALESCE(p.doc, r.doc) = nv2.emp_doc
-- opcional: se quiser restringir a saída final só a PJ, adicione WHERE tp='J'
;

