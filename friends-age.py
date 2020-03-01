from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("friends by age")
sc = SparkContext(conf = conf)

def parsing(x):
    cols = x.split(",")
    age = cols[2]
    friends = int(cols[3])
    return(age,friends)

lines = sc.textFile("file:///SparkCourse/data/fakefriends.csv")
parsedline = lines.map(parsing)

rdd1 = parsedline.mapValues(lambda x: (x,1))
friends = rdd1.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))

result = friends.collect()
for eachdata in result:
    print("Age:{} \t Friends:{}".format(eachdata[0], eachdata[1][0]))
