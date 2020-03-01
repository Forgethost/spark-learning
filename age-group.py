from pyspark.sql import SparkSession
from pyspark.sql import Row

#----------------
#this below configuraton is mandatory for windows run
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("SparkSQL").getOrCreate()
#-----------------
def parsing(x):
    fields = x.split(",")
    return Row(Id=int(fields[0]), name = fields[1], age=int(fields[2]), numFriends=int(fields[3]))

lines = spark.sparkContext.textFile("file:///SparkCourse/data/fakefriends.csv")
people = lines.map(parsing)

schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("friends")

teenagers = spark.sql("select * from friends where age between 10 and 19")

for teen in teenagers.collect():
    print(teen)


#show() will print top 20 rows
schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()