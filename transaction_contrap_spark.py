import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime

# ============================================================================
# CONFIGURAÇÃO INICIAL
# ============================================================================

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'DATABASE_NAME',             # Nome do database no Glue Catalog
    'TABLE_TRANSACOES',          # Nome da tabela de transações
    'TABLE_SOCIOS',              # Nome da tabela de sócios
    'S3_OUTPUT_PATH',            # s3://bucket/path/to/output/
    'S3_CHECKPOINT_PATH',        # s3://bucket/path/to/checkpoints/
    'PARTITION_FILTER_TX',       # Ex: (ano='2024' AND mes='11')
    'PARTITION_FILTER_SOC'       # Ex: (ano='2024')
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ============================================================================
# CONFIGURAÇÕES AVANÇADAS PARA BIG DATA (9 BILHÕES DE REGISTROS)
# ============================================================================

# Adaptive Query Execution (crítico para skew)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

# Shuffle otimizado para bilhões de registros
spark.conf.set("spark.sql.shuffle.partitions", "4000")  # Aumentado para 9B registros
spark.conf.set("spark.default.parallelism", "4000")

# Broadcast ajustado (sócios = 50M pode ser grande demais para broadcast)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50MB")  # Conservador

# Gerenciamento de memória
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.3")
spark.conf.set("spark.sql.files.maxPartitionBytes", "256MB")  # Partições maiores

# Otimizações de I/O
spark.conf.set("spark.sql.parquet.mergeSchema", "false")
spark.conf.set("spark.sql.parquet.filterPushdown", "true")
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")

# Compressão agressiva para shuffle
spark.conf.set("spark.sql.shuffle.compress", "true")
spark.conf.set("spark.io.compression.codec", "snappy")

# Dynamic Partition Pruning (crítico para partições)
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")

# Checkpointing
checkpoint_dir = args['S3_CHECKPOINT_PATH']
spark.sparkContext.setCheckpointDir(checkpoint_dir)

def log_step(step_name, df=None, show_count=False):
    """Função auxiliar para logging consistente"""
    print(f"\n{'='*80}")
    print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {step_name}")
    if df is not None:
        print(f"   📊 Partições: {df.rdd.getNumPartitions()}")
        print(f"   💾 Cached: {df.is_cached}")
        if show_count:
            count = df.count()
            print(f"   🔢 Registros: {count:,}")
    print(f"{'='*80}\n")

# ============================================================================
# ETAPA 1: CARREGAMENTO DO GLUE DATA CATALOG COM PUSHDOWN
# ============================================================================

log_step("ETAPA 1: Carregando transações do Glue Catalog")

# Leitura otimizada do Catalog com pushdown predicate
datasource_tx = glueContext.create_dynamic_frame.from_catalog(
    database=args['DATABASE_NAME'],
    table_name=args['TABLE_TRANSACOES'],
    transformation_ctx="datasource_tx",
    push_down_predicate=args.get('PARTITION_FILTER_TX', '')
)

# Converter para DataFrame Spark
df_transacoes_raw = datasource_tx.toDF()

print(f"✅ Partições carregadas: {df_transacoes_raw.rdd.getNumPartitions()}")
print(f"✅ Schema: {len(df_transacoes_raw.columns)} colunas")

# ---

log_step("ETAPA 1b: Carregando sócios do Glue Catalog")

datasource_soc = glueContext.create_dynamic_frame.from_catalog(
    database=args['DATABASE_NAME'],
    table_name=args['TABLE_SOCIOS'],
    transformation_ctx="datasource_soc",
    push_down_predicate=args.get('PARTITION_FILTER_SOC', '')
)

df_socios_raw = datasource_soc.toDF()

print(f"✅ Partições carregadas: {df_socios_raw.rdd.getNumPartitions()}")

# ============================================================================
# ETAPA 2: TRANSFORMAÇÃO - TRANSAÇÕES COM SALTING
# ============================================================================

log_step("ETAPA 2: Transformando transações (tx) com salting para evitar skew")

df_transacoes_raw.createOrReplaceTempView("base_transacoes")

# Adicionar salt para distribuir melhor documentos populares (ex: grandes varejistas)
df_tx = spark.sql("""
    SELECT
        REGEXP_REPLACE(num_cpf_cnpj_emio, '[^0-9]', '') AS em_doc,
        UPPER(cod_tipo_pess_emio)                        AS em_tp,
        REGEXP_REPLACE(num_cpf_cnpj_favo, '[^0-9]', '') AS fv_doc,
        UPPER(cod_tipo_pess_favo)                        AS fv_tp,
        CAST(vlr_pix AS DOUBLE)                          AS vlr_pix,
        CAST(RAND() * 10 AS INT)                         AS salt_key
    FROM base_transacoes
    WHERE REGEXP_REPLACE(num_cpf_cnpj_emio, '[^0-9]', '') IS NOT NULL
      AND REGEXP_REPLACE(num_cpf_cnpj_favo, '[^0-9]', '') IS NOT NULL
      AND vlr_pix > 0
""")

# Reparticionar por documento para otimizar agregações
df_tx = df_tx.repartition(4000, "em_doc", "fv_doc", "salt_key")

# CHECKPOINT: Salvar estado para fault tolerance (9B registros!)
log_step("Criando checkpoint de transações transformadas")
df_tx = df_tx.checkpoint(eager=True)
df_tx.createOrReplaceTempView("tx")

log_step("Resultado tx checkpointed", df_tx, show_count=True)

# ============================================================================
# ETAPA 3: TRANSFORMAÇÃO - SÓCIOS COM DEDUPLICAÇÃO OTIMIZADA
# ============================================================================

log_step("ETAPA 3: Transformando sócios (soc) com deduplicação")

df_socios_raw.createOrReplaceTempView("base_quadro_soc")

# Usar window function é mais eficiente que DISTINCT para 50M registros
df_soc = spark.sql("""
    SELECT 
        emp_doc,
        emp_tp,
        soc_doc,
        soc_tp
    FROM (
        SELECT
            REGEXP_REPLACE(num_cnpj_estb,     '[^0-9]', '') AS emp_doc,
            UPPER(cod_tipo_pess)                            AS emp_tp,
            REGEXP_REPLACE(num_cpf_cnpj_scio, '[^0-9]', '') AS soc_doc,
            UPPER(cod_tipo_pess_scio)                       AS soc_tp,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    REGEXP_REPLACE(num_cnpj_estb, '[^0-9]', ''),
                    REGEXP_REPLACE(num_cpf_cnpj_scio, '[^0-9]', '')
                ORDER BY cod_tipo_pess
            ) AS rn
        FROM base_quadro_soc
        WHERE REGEXP_REPLACE(num_cnpj_estb, '[^0-9]', '') IS NOT NULL
          AND REGEXP_REPLACE(num_cpf_cnpj_scio, '[^0-9]', '') IS NOT NULL
    )
    WHERE rn = 1
""")

# Reparticionar para otimizar JOINs
df_soc = df_soc.repartition(1000, "emp_doc", "soc_doc")

# CACHE: 50M de sócios únicos cabe em memória distribuída
df_soc.cache()
df_soc.createOrReplaceTempView("soc")

log_step("Resultado soc", df_soc, show_count=True)

# ============================================================================
# ETAPA 4: PRÉ-AGREGAÇÃO OTIMIZADA - RECEBIMENTOS
# ============================================================================

log_step("ETAPA 4: Agregando recebimentos (rec_) com salting")

# Agregar com salt, depois consolidar
df_rec_salted = spark.sql("""
    SELECT 
        fv_doc AS doc, 
        fv_tp AS tp,
        salt_key,
        SUM(vlr_pix) AS vlr_recebido, 
        COUNT(*) AS qtd_recebida
    FROM tx
    GROUP BY fv_doc, fv_tp, salt_key
""")

# Consolidar salt (remover duplicação por salt)
df_rec = df_rec_salted.groupBy("doc", "tp").agg(
    sum("vlr_recebido").alias("vlr_recebido"),
    sum("qtd_recebida").alias("qtd_recebida")
)

# Reparticionar para broadcast join (resultado menor)
df_rec = df_rec.repartition(500, "doc", "tp")

df_rec.cache()
df_rec.createOrReplaceTempView("rec_")

log_step("Resultado rec_", df_rec, show_count=True)

# ============================================================================
# ETAPA 5: PRÉ-AGREGAÇÃO OTIMIZADA - PAGAMENTOS
# ============================================================================

log_step("ETAPA 5: Agregando pagamentos (pag_) com salting")

df_pag_salted = spark.sql("""
    SELECT 
        em_doc AS doc, 
        em_tp AS tp,
        salt_key,
        SUM(vlr_pix) AS vlr_pago, 
        COUNT(*) AS qtd_paga
    FROM tx
    GROUP BY em_doc, em_tp, salt_key
""")

df_pag = df_pag_salted.groupBy("doc", "tp").agg(
    sum("vlr_pago").alias("vlr_pago"),
    sum("qtd_paga").alias("qtd_paga")
)

df_pag = df_pag.repartition(500, "doc", "tp")

df_pag.cache()
df_pag.createOrReplaceTempView("pag_")

log_step("Resultado pag_", df_pag, show_count=True)

# Liberar tx após uso (9B registros!)
df_tx.unpersist()
print("🗑️  DataFrame tx (checkpointed) liberado da memória")

# ============================================================================
# ETAPA 6: NÍVEL 1 - AGREGAÇÃO DOS SÓCIOS (COM BROADCAST HINT)
# ============================================================================

log_step("ETAPA 6: Nível 1 - Agregando movimentação dos sócios")

# BROADCAST HINT: rec_ e pag_ são menores após agregação
df_socio_global = spark.sql("""
    SELECT
        s.emp_doc,
        s.emp_tp,
        COALESCE(SUM(r.vlr_recebido), 0) AS vlr_recm_totl_scio,
        COALESCE(SUM(r.qtd_recebida), 0) AS qtd_recm_totl_scio,
        COALESCE(SUM(p.vlr_pago),     0) AS vlr_pgto_totl_scio,
        COALESCE(SUM(p.qtd_paga),     0) AS qtd_pgto_totl_scio
    FROM soc s
    LEFT JOIN rec_ /*+ BROADCAST(rec_) */ r
        ON r.doc = s.soc_doc AND r.tp = s.soc_tp
    LEFT JOIN pag_ /*+ BROADCAST(pag_) */ p
        ON p.doc = s.soc_doc AND p.tp = s.soc_tp
    GROUP BY s.emp_doc, s.emp_tp
""")

df_socio_global = df_socio_global.repartition(500, "emp_doc")
df_socio_global.cache()
df_socio_global.createOrReplaceTempView("socio_global_por_empresa")

log_step("Resultado socio_global_por_empresa", df_socio_global, show_count=True)

# ============================================================================
# ETAPA 7: IDENTIFICAÇÃO - EMPRESAS COM SÓCIO EM COMUM (OTIMIZADO)
# ============================================================================

log_step("ETAPA 7: Identificando empresas com sócio em comum")

# Self-join pesado - usar estratégia de bucketing virtual
df_emp_emp = spark.sql("""
    SELECT DISTINCT
        s1.emp_doc AS emp_a,
        s2.emp_doc AS emp_b
    FROM soc s1
    JOIN soc s2
        ON s1.soc_doc = s2.soc_doc
       AND s1.soc_tp  = s2.soc_tp
       AND s1.emp_doc < s2.emp_doc
""")

# Gerar pares bidirecionais
df_emp_emp_reverse = df_emp_emp.select(
    col("emp_b").alias("emp_a"),
    col("emp_a").alias("emp_b")
)

df_emp_emp_full = df_emp_emp.union(df_emp_emp_reverse)

df_emp_emp_full = df_emp_emp_full.repartition(1000, "emp_a")

# CHECKPOINT: Self-join é operação cara
df_emp_emp_full = df_emp_emp_full.checkpoint(eager=True)
df_emp_emp_full.createOrReplaceTempView("emp_emp_mesmo_socio")

log_step("Resultado emp_emp_mesmo_socio", df_emp_emp_full, show_count=True)

# Liberar soc após uso
df_soc.unpersist()
print("🗑️  DataFrame soc liberado da memória")

# ============================================================================
# ETAPA 8: NÍVEL 2 - AGREGAÇÃO DE EMPRESAS DOS SÓCIOS
# ============================================================================

log_step("ETAPA 8: Nível 2 - Agregando movimentação de empresas associadas")

# rec_ e pag_ já estão em cache, filtrados por tp='J'
df_rec_pj = df_rec.filter(col("tp") == "J")
df_pag_pj = df_pag.filter(col("tp") == "J")

# Criar views temporárias
df_rec_pj.createOrReplaceTempView("rec_pj")
df_pag_pj.createOrReplaceTempView("pag_pj")

df_empresas_socios = spark.sql("""
    SELECT
        m.emp_a AS emp_doc,
        COALESCE(SUM(r.vlr_recebido), 0) AS vlr_recm_totl_empr_asdo,
        COALESCE(SUM(r.qtd_recebida), 0) AS qtd_recm_totl_empr_asdo,
        COALESCE(SUM(p.vlr_pago),     0) AS vlr_pgto_empr_asdo,
        COALESCE(SUM(p.qtd_paga),     0) AS qtd_pgto_empr_asdo
    FROM emp_emp_mesmo_socio m
    LEFT JOIN rec_pj /*+ BROADCAST(rec_pj) */ r 
        ON r.doc = m.emp_b
    LEFT JOIN pag_pj /*+ BROADCAST(pag_pj) */ p 
        ON p.doc = m.emp_b
    GROUP BY m.emp_a
""")

df_empresas_socios = df_empresas_socios.repartition(500, "emp_doc")
df_empresas_socios.cache()
df_empresas_socios.createOrReplaceTempView("empresas_dos_socios_por_empresa")

log_step("Resultado empresas_dos_socios_por_empresa", df_empresas_socios, show_count=True)

# Liberar intermediários
df_emp_emp_full.unpersist()
df_rec.unpersist()
df_pag.unpersist()
print("🗑️  DataFrames intermediários liberados")

# ============================================================================
# ETAPA 9: CONSOLIDAÇÃO FINAL OTIMIZADA
# ============================================================================

log_step("ETAPA 9: Gerando resultado consolidado final")

# Recriar views para o join final
df_rec.createOrReplaceTempView("rec_")
df_pag.createOrReplaceTempView("pag_")

df_resultado = spark.sql("""
    SELECT
        COALESCE(p.doc, r.doc) AS doc,
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
    LEFT JOIN socio_global_por_empresa nv1
        ON COALESCE(p.doc, r.doc) = nv1.emp_doc
       AND COALESCE(p.tp,  r.tp)  = nv1.emp_tp
    LEFT JOIN empresas_dos_socios_por_empresa nv2
        ON COALESCE(p.doc, r.doc) = nv2.emp_doc
""")

log_step("Resultado final", df_resultado)

# Liberar cache final
df_socio_global.unpersist()
df_empresas_socios.unpersist()
print("🗑️  Todos os caches liberados")

# ============================================================================
# ETAPA 10: SALVAMENTO OTIMIZADO COM PARTICIONAMENTO DINÂMICO
# ============================================================================

log_step("ETAPA 10: Preparando para salvamento otimizado")

# Adicionar coluna de partição por faixa de documento (para melhor distribuição)
df_resultado = df_resultado.withColumn(
    "doc_prefix",
    substring(col("doc"), 1, 2)  # Primeiros 2 dígitos do documento
)

# Reparticionar estrategicamente
df_resultado = df_resultado.repartition(500, "tp", "doc_prefix")

# Converter para DynamicFrame para aproveitar otimizações do Glue
dynamic_frame_resultado = DynamicFrame.fromDF(
    df_resultado, 
    glueContext, 
    "dynamic_frame_resultado"
)

# ---

log_step("ETAPA 11: Salvando no S3 com particionamento otimizado")

output_path = args['S3_OUTPUT_PATH']

# Usar Glue para escrita otimizada
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame_resultado,
    connection_type="s3",
    connection_options={
        "path": output_path,
        "partitionKeys": ["tp", "doc_prefix"]
    },
    format="parquet",
    format_options={
        "compression": "snappy",
        "blockSize": 268435456,  # 256MB
        "pageSize": 1048576      # 1MB
    },
    transformation_ctx="write_resultado"
)

print(f"✅ Dados salvos com sucesso em: {output_path}")

# ============================================================================
# ESTATÍSTICAS FINAIS
# ============================================================================

print("\n" + "="*80)
print("📈 RESUMO DO PROCESSAMENTO - 9 BILHÕES DE TRANSAÇÕES")
print("="*80)
print(f"Caminho de saída: {output_path}")
print(f"Checkpoint dir: {checkpoint_dir}")
print(f"Partições finais: 500")
print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ============================================================================
# FINALIZAÇÃO
# ============================================================================

job.commit()
print("\n🎉 Job finalizado com sucesso!")
print("💡 Verifique CloudWatch Metrics para análise de performance")
