from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
import sys

# replace host cassandra !!! 
spark = SparkSession.builder\
            .config("spark.app.name", "StockAnalyzer")\
            .config("spark.master", "spark://spark-master:7077")\
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0")\
            .config("spark.cassandra.connection.host", "172.18.0.9")\
            .config("spark.cassandra.auth.username", "cassandra")\
            .config("spark.cassandra.auth.password", "cassandra")\
            .enableHiveSupport()\
            .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# read data from cassandra
result_df = spark.read.format('org.apache.spark.sql.cassandra')\
            .options(table='stock_data', keyspace='stock')\
            .load()

#Xu ly them column % thay doi cua ma co phieu
data_df = result_df.withColumn("change", (result_df.close - result_df.open)/result_df.open*100)

def statistic (date):
    #Thong ke
    #Top ma tang nhieu nhat
    print("Top 10 ma co phieu tang nhieu nhat ngay {}".format(date))
    data_df.filter(to_date(data_df.trading_date) == date).sort(data_df.change.desc()).show(10)

    #Top giam nhieu nhat
    print("Top 10 ma co phieu giam nhieu nhat ngay {}".format(date))
    data_df.filter(to_date(data_df.trading_date) == date).sort(data_df.change.asc()).show(10)

    #Top ma co volumne lon nhat
    print("Top 10 ma co phieu co volumne lon nhat ngay {}".format(date))
    data_df.filter(to_date(data_df.trading_date) == date).sort(data_df.volume.desc()).show(10)


# def top_transaction_value(date):
#     print("Top 10 cổ phiếu có giá trị giao dịch lớn nhất ngày {}".format(date))
#     data_df.withColumn("transaction_value", data_df.volume * data_df.close)\
#         .filter(to_date(data_df.trading_date) == date)\
#         .sort(col("transaction_value").desc())\
#         .select("symbol", "transaction_value")\
#         .show(10)

# def price_change_summary(date):
#     print("Tóm tắt tỷ lệ tăng/giảm cổ phiếu ngày {}".format(date))
#     summary_df = data_df.filter(to_date(data_df.trading_date) == date)\
#         .withColumn("status", 
#             when(data_df.change > 0, "increase")
#             .when(data_df.change < 0, "decrease")
#             .otherwise("unchanged"))\
#         .groupBy("status")\
#         .count()
#     summary_df.show()

# def daily_extremes(date):
#     print("Cổ phiếu đạt mức giá cao nhất ngày {}".format(date))
#     data_df.filter(to_date(data_df.trading_date) == date)\
#         .sort(data_df.high.desc())\
#         .select("symbol", "high")\
#         .show(1)

#     print("Cổ phiếu đạt mức giá thấp nhất ngày {}".format(date))
#     data_df.filter(to_date(data_df.trading_date) == date)\
#         .sort(data_df.low.asc())\
#         .select("symbol", "low")\
#         .show(1)

# def average_change(date):
#     print("Biến động trung bình của tất cả cổ phiếu ngày {}".format(date))
#     avg_change = data_df.filter(to_date(data_df.trading_date) == date)\
#         .agg(avg("change").alias("avg_change"))
#     avg_change.show()

# def most_stable_stocks(date):
#     print("Top 10 cổ phiếu ổn định nhất ngày {}".format(date))
#     data_df.withColumn("price_range", data_df.high - data_df.low)\
#         .filter(to_date(data_df.trading_date) == date)\
#         .sort(col("price_range").asc())\
#         .select("symbol", "price_range")\
#         .show(10)

# def time_series_analysis(ticker, period="week"):
#     print("Phân tích biến động {} của cổ phiếu {}".format(period, ticker))
#     period_format = {
#         "week": "weekofyear",
#         "month": "month",
#         "year": "year"
#     }
#     if period not in period_format:
#         print("Period không hợp lệ. Vui lòng chọn 'week', 'month', hoặc 'year'.")
#         return
    
#     time_df = data_df.filter(data_df.symbol == ticker)\
#         .withColumn(period, date_format(data_df.trading_date, period_format[period]))\
#         .groupBy(period)\
#         .agg(avg("change").alias("avg_change"))
#     time_df.show()

# def unusual_volume(date, threshold=2.0):
#     print("Cổ phiếu có khối lượng giao dịch bất thường ngày {}".format(date))
#     avg_volumes = data_df.groupBy("symbol").agg(avg("volume").alias("avg_volume"))
#     joined_df = data_df.filter(to_date(data_df.trading_date) == date)\
#         .join(avg_volumes, "symbol")\
#         .withColumn("volume_ratio", data_df.volume / col("avg_volume"))\
#         .filter(col("volume_ratio") > threshold)
    # joined_df.select("symbol", "volume", "avg_volume", "volume_ratio").show()

def history (ticker):
    #Lich su
    #Xem lich su 1 ma
    print("Lich su ma co phieu {} 10 ngay gan nhat".format(ticker))
    data_df.filter(data_df.symbol == ticker).show(10)

if (sys.argv[1] == "statistic"):
    statistic(sys.argv[2])
elif (sys.argv[1] == "history"):
    history(sys.argv[2])
# elif (sys.argv[1] == "top_transaction_value"):
#     top_transaction_value(sys.argv[2])
# elif (sys.argv[1] == "price_change_summary"):
#     price_change_summary(sys.argv[2])
# elif (sys.argv[1] == "daily_extremes"):
#     daily_extremes(sys.argv[2])
# elif (sys.argv[1] == "average_change"):
#     average_change(sys.argv[2])
# elif (sys.argv[1] == "most_stable_stocks"):
#     most_stable_stocks(sys.argv[2])
# elif (sys.argv[1] == "time_series_analysis"):
#     time_series_analysis(sys.argv[2], sys.argv[3])
# elif (sys.argv[1] == "unusual_volume"):
#     unusual_volume(sys.argv[2], float(sys.argv[3]))
else:
    print("Invalid analysis type!")






