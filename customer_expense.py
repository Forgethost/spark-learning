from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("customer expense")
sc = SparkContext(conf = conf)

def parseline(x):
    fields = x.split(",")
    id = int(fields[0])
    expense = float(fields[2])
    return (id, expense)

lines = sc.textFile("file:///SparkCourse/data/customer-orders.csv")
parsedline = lines.map(parseline)
results = parsedline.reduceByKey(lambda x,y: x+y)
newrdd = results.map(lambda x: (x[1],x[0]))
newrddsorted = newrdd.sortByKey()
final_result = newrddsorted.collect()

for k, v in final_result:
    print("{},{:.2f}".format(v,k))