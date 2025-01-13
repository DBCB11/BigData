from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys


# replace host cassandra !!! 
spark = SparkSession.builder\
            .config("spark.app.name", "StockDataAnalyzer")\
            .config("spark.master", "spark://spark-master:7077")\
            .config("spark.cores.max", "6") \
            .config("spark.executor.memory", "512m") \
            .config("spark.driver.memory", "512m") \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0")\
            .config("spark.cassandra.connection.host", "cassandra")\
            .enableHiveSupport()\
            .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.format('csv')\
    .option('header', True)\
    .option('inferSchema', True)\
    .load("hdfs://namenode:9000/stockData/"+sys.argv[1])
print(df.head(10))
df.show()

df = df.select(
    col('Symbol').alias('symbol'),
    col(' Trading date').alias('trading_date'),
    col(' High').alias('high'), col(' Low').alias('low'), col(' Open').alias('open'), col(' Close').alias('close'), col(' Volume').alias('volume')
)

# Ghi dữ liệu vào Cassandra
print("Writing data to Cassandra...")
try:
    df.write.format('org.apache.spark.sql.cassandra')\
        .mode('append')\
        .options(table='stock_data', keyspace='stock')\
        .save()
    print("Data successfully written to Cassandra table 'stock_data' in keyspace 'stock'.")
except Exception as e:
    print(f"An error occurred while writing data to Cassandra: {e}", file=sys.stderr)
