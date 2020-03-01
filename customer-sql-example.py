from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *


def mapper(lines):
    row = lines.split(",")
    return [row[0].strip(), row[1].strip(), float(row[2])]


spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("water-quality").getOrCreate()

schema = StructType([StructField("cust_id", StringType(), True),
                     StructField("product_id", StringType(), True),
                     StructField("amount", FloatType(), True)])
orders_rdd = spark.sparkContext.textFile("C:\\SparkCourse\\data\\customer-orders.csv")
orders = orders_rdd.map(mapper)
order_df = spark.createDataFrame(orders, schema)
order_df.createOrReplaceTempView("customer_orders")
#order_df = spark.createDataFrame(orders_rdd, schema)

#customer_order = sqlContext.read.format("com.databricks.spark.csv").options(header='false').load("C:\SparkCourse\data\customer-orders.csv")
top_10_customers = spark.sql("""select cust_id, count(*) from customer_orders
                                group by cust_id 
                                order by count(*) desc limit 10""")
#customer_order.groupBy("_c0").count().show()
#order_df.printSchema()
top_10_customers.show()
top_10_customers.write.text("c:\\sparkcourse\data\\top10customers.txt")