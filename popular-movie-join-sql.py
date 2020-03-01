from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *

def user_item_mapper(lines):
    col = lines.decode().split("|")
    return Row(
        movie = int(col[0]),
        moviename = str(col[1])
    )

schema = StructType(
    [StructField("userid",IntegerType()),
     StructField("movieid", IntegerType()),
     StructField("rating", IntegerType()),
     StructField("timestamp", DateType())
     ]
)

spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///c:/temp").appName("popular-movie").master("local").getOrCreate()
item_rdd = spark.sparkContext.textFile("file:///c:/sparkcourse/ml-100k/ml-100k/item.txt").map(user_item_mapper)
print(item_rdd.take(5))
item_df = spark.createDataFrame(item_rdd)

#user_rdd = spark.sparkContext.textFile("file:///c:/sparkcourse/ml-100k/ml-100k/user.txt")
#user_df = spark.createDataFrame(user_rdd,schema=schema)

#user_df.printSchema()
#df = user_df.join(item_df,user_df.movieid==item_df.movie)
item_df.select("moviename").count().show()