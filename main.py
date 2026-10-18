# toda sessão PySpark começa criando uma SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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
spark.stop()

