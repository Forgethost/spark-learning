import sys
from pyspark import SparkContext, SparkConf
from pyspark.mllib.recommendation import ALS, Rating

def names():
    namedict = dict()
    file = open("./ml-100k/ml-100k/u.item")
    for record in file:
        fields = record.split("|")
        namedict[(int(fields[0]))] = fields[1].encode(encoding="ascii",errors="ignore")
    return namedict

conf = SparkConf().setMaster("local[*]").setAppName("ALS recommendation")
sc = SparkContext(conf=conf)

namedict = names()
lines = sc.textFile("file:///SparkCourse/ml-100k/ml-100k/u.data")
train_data = lines.map(lambda x: x.split()).map(lambda x: Rating(int(x[0]), int(x[1]), x[2])).cache()

numiter = 10
rank = 10

print("Training model")
model = ALS.train(train_data,rank,numiter)

id = int(sys.argv[1])

movies = train_data.filter(lambda x: x[0]==id)

for i in movies.collect():
    print(namedict[i[1]])

#top 10 recommendation
recommendation = model.recommendProducts(id, 10)

for i in recommendation:
    print(namedict[i[1]],"score=",i[2])