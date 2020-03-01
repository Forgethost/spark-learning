from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("popular superhero")
sc = SparkContext(conf = conf)

def parsing(x):
    fields = x.split("\"")
    id = int(fields[0])
    name = fields[1]
    return  (id, name)

def countsperline(x):
    fields = x.split()
    id = int(fields[0])
    id_occurence = len(fields) - 1
    return  (id, int(id_occurence))

namerdd = sc.textFile("file:///SparkCourse/data/marvel-names.txt")
nameformat = namerdd.map(parsing)

superherordd = sc.textFile("file:///SparkCourse/data/marvel-graph.txt")

superherocountsrdd = superherordd.map(countsperline)
superhero_counts = superherocountsrdd.reduceByKey(lambda x,y: x+y )
superhero_counts_flipped = superhero_counts.map(lambda x: (x[1], x[0]))

#use max on rdd to et the max key,value pair based on key
mostpopular = superhero_counts_flipped.max()

#use loopup on rdd to find key,value pair with the key value
mostpopularname = nameformat.lookup(mostpopular[1])

print("Most popular superhero",mostpopularname[0], "occurence", mostpopular[0])