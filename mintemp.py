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
filterdline = parsedline.filter(lambda x: "TMIN" in x)
tempdata = filterdline.map(lambda x: (x[0], x[2]))
mintemp = tempdata.reduceByKey(lambda x,y: min(x,y))

results = mintemp.collect()
for each in results:
    print(each[0], "{:.2f}C".format(each[1]))