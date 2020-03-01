from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("SparkSQL").getOrCreate()
def names(record):
    fields = record.split("|")
    return(int(fields[0]),fields[1])


nameline = spark.sparkContext.textFile("file:///SparkCourse/ml-100k/ml-100k/u.item")
movie_names = nameline.map(names)
lines = spark.sparkContext.textFile("file:///SparkCourse/ml-100k/ml-100k/u.data")
movies = lines.map(lambda x: Row(Id=int(x.split()[1])))

movies_df = spark.createDataFrame(movies)

popular_movies = movies_df.groupBy("Id").count().orderBy("count", ascending=False).cache()
popular_movies.show()

popular_movies10 = popular_movies.take(10)

for movie in popular_movies10:
    print(movie_names.lookup(movie[0]), movie[1])
spark.stop()