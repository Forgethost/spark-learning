from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("SparkSQL").getOrCreate()

lines1 = spark.sparkContext.textFile("file:///SparkCourse/ml-100k/ml-100k/u.data")
datarow1 = lines1.map(lambda x: x.split()).map(lambda x: Row(userid=int(x[0]),movieid = int(x[1]),
                                                             rating = int(x[2]), time=x[3]))
schema1 = spark.createDataFrame(datarow1).cache()
schema1.createOrReplaceTempView("user_rating")

lines2 = spark.sparkContext.textFile("file:///SparkCourse/ml-100k/ml-100k/u.item")
datarow2 = lines2.map(lambda x: x.split("|")).map(lambda x: Row(movieid=int(x[0]),
            moviename = x[1],action = int(x[6])))


schema2 = spark.createDataFrame(datarow2).cache()
schema2.createOrReplaceTempView("movie_names")

movies = spark.sql("""select movie_rating.movieid, moviename, movie_rating.rate
                      from movie_names inner join
                      (select movieid, max(rating) as rate from user_rating 
                       group by movieid) movie_rating
                       on movie_names.movieid = movie_rating.movieid
                       where action = 1""")

for rows in movies.take(20):
    print(rows)

