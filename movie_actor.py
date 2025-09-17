from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, mode, round,to_date, sum, count, year
spark = SparkSession.builder \
.appName("read") \
.getOrCreate()

#Загрузка датафрейма
actors_df = spark.read.option("header","true").csv("/content/actors.csv")
movies_df = spark.read.option("header","true").csv("/content/movies.csv")
ma_df = spark.read.option("header","true").csv("/content/movie_actors.csv")

actors_df.createOrReplaceTempView("actors")
movies_df.createOrReplaceTempView("movies")
ma_df.createOrReplaceTempView("movie_actors")

temp_df = spark.sql(
"select * from movies join movie_actors on movie_actors.movie_id = movies.movie_id join actors on movie_actors.actor_id = actors.actor_id"
)
temp_df = temp_df.drop("actor_id")
temp_df = temp_df.drop("movie_id") # это всё костыль, так делать на реальных данных нельзя, но тут оно сработает стопроц
temp_df.show()

#Найдите топ-5 жанров по количеству фильмов.
genre_df = movies_df.groupBy("genre").agg(count("movie_id").alias("num_movies")).orderBy(col("num_movies").desc())
genre_df.show()

#Найдите актера с наибольшим количеством фильмов
topact_df = temp_df.groupBy("name").agg(count("title").alias("num_movies")).orderBy(col("num_movies").desc()).limit(1) 
topact_df.show()

#Средний бюджет фильмов по жанрам
avgbud_df = movies_df.groupBy("genre").agg(avg("budget").alias("avg_budget")) 
avgbud_df.show()

#Найдите фильмы, в которых снялось более одного актера из одной страны
temp_df.createOrReplaceTempView("temp")
last_df = spark.sql("select title, country, count(name) as num_actors from temp group by title, country having count(name)>1 ")
last_df.show()
