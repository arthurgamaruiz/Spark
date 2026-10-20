# toda sessão PySpark começa criando uma SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round
from pyspark.sql.functions import count, when, isnan, lit, sum, avg, min, max
"""
*****************************************************
**************** SELEÇÃO DE COLUNAS *****************
*****************************************************
"""
# Escolher quais colunas você quer visualizar ou processar. Equivalente ao SELECT do SQL.
spark = (
    SparkSession.builder
    .appName("EstudoPySpark")
    .master("local[2]")     # duas threads 
    .getOrCreate()
)

# Leitura do arquivo CSV (separado por vírgula)
df = (
    spark.read
    .option("header", "true")           # primeira linha é cabeçalho
    .option("sep", ",")                 # delimitador vírgula
    .option("inferSchema", "true")      # Spark tenta adivinhar os tipos
    .csv("dados/dados_pyspark.txt")
)

# Ver a estrutura
print("**** ESTRUTURA ****")
df.printSchema()

# Ver as primeiras linhas
df.show(5, truncate=False)

# Contar registros
print(f"Total de registros: {df.count()}")

# Selecionar colunas específicas
df.select('cliente', 'cidade', 'produto', 'preco_unitario').show(10)

# Função col()
df.select(col('cliente'), col('preco_unitario'), col('quantidade')).show(10)

"""
******************************************
**************** FILTROS *****************
******************************************
"""
# Selecionar apenas os registros que atendem a uma condição. Equivalente ao WHERE do SQL.

# Função filter
df.filter(col('quantidade') > 1).select('cliente','preco_unitario','quantidade').show()

# Pedidos com preço unitário maior que 1000
df.filter(col('preco_unitario') > 1000).select('cliente','preco_unitario', 'produto').show()

# Pedidos com eletrônicos - coluna categoria
df.filter(col('categoria') == 'Eletrônicos').select('produto', 'data_pedido', 'quantidade').show()

# Eletrônicos com preço superior a 500
df.filter((col('categoria') == 'Eletrônicos') & (col('preco_unitario') > 500)).select('produto', 'data_pedido','quantidade','preco_unitario').show()

# Pedidos cancelados OU devolvidos
df.filter(
    (col("status_pedido") == "Cancelado") | (col("status_pedido") == "Devolvido")
).select('status_pedido', 'produto','cliente').show(10)

# Atalho com isin()
df.filter(col("status_pedido").isin("Cancelado", "Devolvido")).select('status_pedido', 'produto','cliente').show(10)

"""
**************************************************************
**************** CRIAR E TRANSFORMAR COLUNAS *****************
**************************************************************
"""
# Adicionar colunas novas ou modificar existentes. Muito usado na etapa T (Transform) do ETL.

# Calcular valor bruto = quantidade x preço unitário
df_com_bruto = df.withColumn(
    'valor_bruto',
    col('quantidade') * col('preco_unitario')
)
df_com_bruto.select('cliente', 'valor_bruto').show(10)

# Valor com desconto
df_com_desconto = df_com_bruto.withColumn(
    'valor_com_desconto',
    spark_round(col('valor_bruto') * (1-col('desconto_pct')), 2)
)
df_com_desconto.select('valor_bruto','desconto_pct','valor_com_desconto').show(10)

# df final
df_final = df_com_desconto.withColumn(
    'valor_final',
    spark_round(col('valor_com_desconto') + col('frete'), 2)
)

df_final.select(
    'pedido_id','produto','quantidade','preco_unitario','desconto_pct',
    'frete','valor_bruto','valor_com_desconto','valor_final'
).show(10, truncate=False)

"""
**************************************************************
**************** TRATAR VALORES NULOS ************************
**************************************************************
"""
# Dados reais vêm com campos vazios. No arquivo, 'cupom' e 'avaliacao' têm valores ausentes. Tratar isso é essencial em qualquer ETL.

# Contar valores nulos por coluna
df.select([
    count(when(col(c).isNull(), c)).alias(c) for c in df.columns
]).show(truncate=False)

# Preencher 'cupom' vazio com 'SEM_CUPOM'
df_tratado = df.fillna({'cupom': 'SEM_CUPOM'})
df_tratado.select('cupom').show()

# Preencher 'avaliacao' vazia com mediana/valor padrão
df_tratado = df_tratado.fillna({'avaliacao': 0})
df_tratado.select('cupom', 'avaliacao').show()

"""
**************************************************************
*********************** AGREGAÇÕES ***************************
**************************************************************
"""

# Agrupar dados e calcular métricas

# Faturamento bruto por categoria
df_faturamento_bruto = df_final.groupBy('categoria').agg(
    spark_round(sum('valor_bruto'), 2).alias('faturamento_bruto'),
    count('pedido_id').alias('total_pedidos')).orderBy(col('faturamento_bruto').desc())

df_faturamento_bruto.select('*').show()

# Análise de vendas por estado
print("**** VENDAS POR ESTADO ****")

df_final.groupBy('estado').agg(
    count('pedido_id').alias('qtd_pedidos'),
    spark_round(sum('valor_final'), 2).alias('receita_estado')
).orderBy(col('receita_estado').desc()).show(10)

spark.stop()