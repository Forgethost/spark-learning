from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("popular")
sc = SparkContext(conf = conf)

def parsing(x):
    fields = x.split()
    movieid = int(fields[1])
    return  (movieid, 1)

lines = sc.textFile("file:///SparkCourse/ml-100k/ml-100k/u.data")
movieid = lines.map(parsing)
movielist = movieid.reduceByKey(lambda x,y: x+y )
movielist_flip = movielist.map(lambda x: (x[1], x[0]))
sortedmovielist = movielist_flip.sortByKey()
results = sortedmovielist.collect()


for value in results:
    print(value)
