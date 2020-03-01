from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("min temperature")
sc = SparkContext(conf = conf)

def parsing(x):
    cols = x.split(",")
    id = cols[0]
    type = cols[2]
    temp = float(cols[3]) * 0.1
    return (id, type, temp)

lines = sc.textFile("file:///SparkCourse/data/1800.csv")
parsedline = lines.map(parsing)
filterdline = parsedline.filter(lambda x: "TMAX" in x)
tempdata = filterdline.map(lambda x: (x[0], x[2]))
maxtemp = tempdata.reduceByKey(lambda x,y: max(x,y))

results = maxtemp.collect()
for each in results:
    print(each[0], "\t{:.1f}C".format(each[1]))