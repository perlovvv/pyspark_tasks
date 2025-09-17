from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, mode, round,to_date, sum, count, year
spark = SparkSession.builder \
.appName("read") \
.getOrCreate()

#Загрузка датафрейма
authors_df = spark.read.option("header","true").csv("/content/authors.csv")
books_df = spark.read.option("header","true").csv("/content/books.csv")
#Изменение формата даты
authors_df = authors_df.withColumn("birth_date", to_date(col("birth_date"), "dd-MM-yyyy"))
books_df = books_df.withColumn("publish_date", to_date(col("publish_date"), "dd-MM-yyyy"))
books_df = books_df.withColumn("publish_date", to_date(col("publish_date"), "dd-MM-yyyy"))
#Объединение авторов и книг по id

ab_df = books_df.join(authors_df, on = "author_id", how = "inner")
#ab_df = ab_df.drop(books_df.author_id)

#Поиск 5 авторов с самой большой выручкой
authors_by_revenue = ab_df.groupBy("author_id", 'name') \
                              .agg(sum("price").alias("total_revenue")) \
                              .orderBy(col("total_revenue").desc()) \
                              .limit(5)
authors_by_revenue.show()

#Поиск количества книг по жанрам
genre_df = books_df.groupBy("genre") \
                   .agg(count("title").alias("count")) \
                   .orderBy(col("count").desc())
genre_df.show()

#Средняя цена книги по каждому автору
avgp_df = ab_df.groupBy("author_id","name") \
                   .agg(avg("price").alias("average_price")) \
                   .orderBy(col("average_price").desc())
avgp_df.show()

#книги выпущенные после 2000г.

books_after_2000 = books_df.filter(col("publish_date") > lit("01-01-2001")).orderBy(col("price").desc())

books_after_2000.show()
