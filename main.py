# toda sessão PySpark começa criando uma SparkSession
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("EstudoPySpark")
    .master("local[2]")
    .getOrCreate()
)

# Leitura do arquivo CSV (separado por vírgula)
df = (
    spark.read
    .option("header", "true")    # primeira linha é cabeçalho
    .option("sep", ",")          # delimitador vírgula
    .option("inferSchema", "true")  # Spark tenta adivinhar os tipos
    .csv("dados/dados_pyspark.txt")
)

# Ver a estrutura
print("**** ESTRUTURA ****")
df.printSchema()

# Ver as primeiras linhas
df.show(5, truncate=False)

# Contar registros
print(f"Total de registros: {df.count()}")

spark.stop()

