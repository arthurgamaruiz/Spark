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
# print("**** ESTRUTURA ****")
# df.printSchema()

# Ver as primeiras linhas
# df.show(5, truncate=False)

# Contar registros
# print(f"Total de registros: {df.count()}")

# Selecionar colunas específicas
# df.select('cliente', 'cidade', 'produto', 'preco_unitario').show(10)

# Função col()
df.select(col('cliente'), col('preco_unitario'), col('quantidade')).show(10)
spark.stop()

