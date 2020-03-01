from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors

if __name__ == "__main__":
    spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("LinearRegression").getOrCreate()
    lines = spark.sparkContext.textFile("file:///SparkCourse/data/regression.txt")
    data = lines.map(lambda x: x.split(",")).map(lambda x: (float(x[0]), Vectors.dense(float(x[1]))))

    colnames = ["label","features"]
    df = data.toDF(colnames)

    splitdf = df.randomSplit([0.5,0.5])
    traindf = splitdf[0]
    testdf = splitdf[1]

    lr= LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
    model = lr.fit(traindf)

    fullpredictions = model.transform(testdf).cache()

    predictions = fullpredictions.select("prediction").rdd.map(lambda x: x[0])
    labels = fullpredictions.select("label").rdd.map(lambda x: x[0])

    concatenated = predictions.zip(labels).collect()

    for i in concatenated:
        print(i)

    spark.stop()

