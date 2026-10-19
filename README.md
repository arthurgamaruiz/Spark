# Estudo de PySpark & ETL para Engenharia de Dados

Ambiente de treino local focado em **Apache Spark (PySpark)** e conceitos de **ETL (Extract, Transform, Load)**, preparando para desafios práticos de engenharia de dados e estágios.

---

## 📁 Estrutura do Projeto

```text
estudo-pyspark/
├── .venv/                   # Ambiente virtual Python
├── dados/
│   ├── dados_pyspark.txt    # Base de dados de treino (500 registros de vendas)
│   └── funcionarios.txt     # Base de dados auxiliar simples
├── saida/                   # Dados gerados após transformações (Parquet)
├── main.py                  # Script principal com os testes e rotinas PySpark
└── README.md                # Documentação do projeto
```

---

## 🚀 Como Executar o Ambiente

1. **Ativar o ambiente virtual (PowerShell):**
   ```powershell
   .\.venv\Scripts\Activate.ps1
   ```

2. **Executar o script de treino:**
   ```powershell
   python main.py
   ```

---

## 📊 Sobre a Base de Dados (`dados_pyspark.txt`)

O arquivo contém **500 registros** simulando um sistema de comércio eletrônico, separados por vírgula (`,`), contendo 18 colunas com diferentes tipos de dados (texto, numéricos, datas e valores nulos):

- **Identificação:** `pedido_id`, `cliente`
- **Demografia/Localização:** `idade_cliente`, `cidade`, `estado`
- **Produto:** `categoria`, `produto`, `quantidade`, `preco_unitario`
- **Comercial:** `desconto_pct`, `frete`, `forma_pagamento`, `status_pedido`
- **Atendimento/Marketing:** `canal_venda`, `dispositivo`, `cupom`, `avaliacao`

---

## 🛠️ Conceitos e Comandos Praticados

### 1. Inicialização e Leitura
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("EstudoPySpark")
    .master("local[2]")
    .getOrCreate()
)

df = (
    spark.read
    .option("header", "true")
    .option("sep", ",")
    .option("inferSchema", "true")
    .csv("dados/dados_pyspark.txt")
)
```

### 2. Transformações (`withColumn`)
Criação de novas métricas a partir de colunas existentes (ex: cálculo de valor bruto, desconto e frete).

### 3. Filtros e Agregações (`filter`, `groupBy`, `agg`)
Filtros condicionais complexos e agrupamentos por categoria, estado e canal de venda para extração de indicadores de negócio.

### 4. Carga (`Load` em Parquet)
Exportação dos dados transformados para o formato otimizado **Parquet**, simulando a camada analítica de um Data Lake.
