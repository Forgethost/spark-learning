from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("popular")
sc = SparkContext(conf = conf)

def names():
    moviename = dict()
    file = open("./ml-100k/ml-100k/u.item")
    for lines in file:
        fields = lines.split("|")
        moviename[int(fields[0])] = fields[1].strip()
    file.close()
    return moviename

def parsing(x):
    fields = x.split()
    movieid = int(fields[1])
    return  (movieid, 1)

moviename_broadcast = sc.broadcast(names())

lines = sc.textFile("file:///SparkCourse/ml-100k/ml-100k/u.data")
movieid = lines.map(parsing)
movielist = movieid.reduceByKey(lambda x,y: x+y )
movielist_flip = movielist.map(lambda x: (x[1], moviename_broadcast.value[x[0]]))
sortedmovielist = movielist_flip.sortByKey()
results = sortedmovielist.collect()


for result in results:
    print(result)


