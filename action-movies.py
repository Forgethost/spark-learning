from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[*]").setAppName("action movies")
sc = SparkContext(conf=conf)
def parse_userdata(x):
    fields = x.split()
    return (int(fields[1]), int(fields[2]))

def action_movie_filter(x):
    fields = x.split("|")
    return int(fields[6]) == 1

def duplicate_ratings(x):
    ratings = x[1]
    hld = []
    for eachrating in ratings:
        if eachrating in hld:
            continue
        else:
            hld.append(eachrating)
    hld.sort(reverse=True)
    return (x[0],hld)

def expand(x):
    movieid = x[0]
    name = x[1]
    ratinglist = x[2]
    temp=[]
    for i in ratinglist:
        temp.append(((movieid,name),i))
    return temp

def format_join(x):
    id = x[0]
    name = x[1][0]
    ratings = x[1][1]
    for each in ratings:
        return (each,id,name)



def action_movie_list(x):
    fields = x.split("|")
    return(int(fields[0]), fields[1].encode("ascii","ignore"))

userdata = sc.textFile("file:///SparkCourse/ml-100k/ml-100k/u.data")
userdatardd = userdata.map(parse_userdata)

lines = sc.textFile("file:///SparkCourse/ml-100k/ml-100k/u.item")
action_movies_filtered = lines.filter(action_movie_filter)
action_movies_rdd = action_movies_filtered.map(action_movie_list)

movie_ratings = userdatardd.groupByKey().map(lambda x: (x[0], x[1]))
movie_ratings_filter = movie_ratings.filter(duplicate_ratings)
action_movie_ratings = action_movies_rdd.join(movie_ratings_filter)
#explode = action_movie_ratings.flatMap(format_join)
test = action_movie_ratings.take(10)
#action_movie_ratings_sorted = explode.sortByKey()

for i in test:
    for j in i[1][1]:
        print(i[0], i[1][0], j)

