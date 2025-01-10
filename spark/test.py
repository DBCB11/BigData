from pyspark.sql.session import SparkSession
# replace host cassandra !!! 
spark = SparkSession.builder\
            .appName("Pyspark Tutorial")\
            .master("spark://localhost:7077")\
            .enableHiveSupport()\
            .getOrCreate()
if spark.sparkContext._jsc.sc().isStopped():
    print("The SparkContext is stopped.")
else:
    print("The SparkContext is active.")
# Print Spark version
print("Spark Version:", spark.version)

# Create a DataFrame with some sample data
data = [("Alice", 25), ("Bob", 30), ("Cathy", 29)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)

# Show the DataFrame
print("Sample DataFrame:")
df.show()

# Perform a simple transformation
df_filtered = df.filter(df.Age > 28)

# Show the transformed DataFrame
print("Filtered DataFrame (Age > 28):")
df_filtered.show()