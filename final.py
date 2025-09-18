from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, mode, round,to_date, sum, count, year
spark = SparkSession.builder \
.appName("read") \
.config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
.getOrCreate()

#Загрузка датафрейма
logs_df = spark.read.option("header","true").csv("/content/web_server_logs.csv")
#logs_df.show()


#часто вызываемые ip
popip_df = logs_df.groupBy("ip").agg(count("ip").alias("reques_count")).orderBy(col("ip").desc()).limit(10)
print("Top 10 active IP addresses")
popip_df.show()

#количество запросов для каждого метода
methodcount_df = logs_df.groupBy("method").agg(count("method").alias("method_count"))
print("Reques count by HTTP method:")
methodcount_df.show()

#Профильтруйте и посчитайте количество запросов с кодом ответа 404
logs_df.createOrReplaceTempView("logs")
error_count_df = spark.sql("select count(*) as count from logs where response_code = 404")
error_count = error_count_df.collect()[0][0]
print(f"Number of 404 RC: {error_count}")
print('')

#Сгруппируйте данные по дате и просуммируйте размер ответов, сортируйте по дате.
print("Total response size by day:")
popdate = spark.sql("""
select
to_date(timestamp,'yyyy-MM-dd') as date,
cast(sum(response_size) as INT)  as total_response_size 
from logs 
group by to_date(timestamp,'yyyy-MM-dd')
order by to_date(timestamp,'yyyy-MM-dd') 
""")
popdate.show(20)
