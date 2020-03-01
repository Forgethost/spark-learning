from pyspark import SparkContext, SparkConf
import collections

conf = SparkConf().setMaster("local").setAppName("movie histogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///SparkCourse/ml-100k/ml-100k/u.data")
user = lines.map(lambda x: x.split()[1])
user_hist = user.countByValue()

sortedResults = collections.OrderedDict(sorted(user_hist.items(), key=lambda x:x[1],reverse=True))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
