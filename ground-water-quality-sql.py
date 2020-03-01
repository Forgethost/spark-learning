from pyspark.sql import SparkSession
from pyspark.sql import Row
#from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField



spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///c://temp"
                                    ).appName("ground-water-qulaity-analysis").master("local").getOrCreate()


def filter_header(lines):
    if lines.split(",")[0].strip() == "STATION CODE":
        return False
    else:
        return  True


def row_mapper(lines):
    columns = lines.split(",")
    return Row(
        station_code=columns[0],
        location=columns[1],
        state = columns[2],
        min_temp = columns[3],
        max_temp = columns[4],
        mean_temp = columns[5],
        min_ph = columns[6],
        max_ph = columns[7],
        mean_ph = columns[8],
        mean_bod = float(columns[14]),
        mean_nitrate = columns[17],
        mean_fecal_coliform = columns[20],
        mean_total_coliform=columns[23]
    )

water_quality_rdd = spark.sparkContext.textFile("C:\\SparkCourse\\data\\ground_water_quality.csv")
water_quality_rdd_filtered = water_quality_rdd.filter(filter_header)
water_quality_schema_rdd = water_quality_rdd_filtered.map(row_mapper)
water_quality_df = spark.createDataFrame(water_quality_schema_rdd)
water_quality_df.createOrReplaceTempView("water_quality")

#water_quality_df.printSchema()
most_polluted_water = spark.sql("""select location from water_quality 
where mean_bod >= (select max(mean_bod) from water_quality)""")

most_polluted_water.show()