from pyspark.sql import SparkSession
spark = SparkSession\
        .builder\
        .appName("sparkratings")\
        .getOrCreate()
sc = spark.sparkContext
#
rddUsers=sc.textFile('/bigdata/books/BX-Users_sample.csv')
rddRatings=sc.textFile('/bigdata/books/BX-Book-Ratings_sample.csv')
#
def func1(x):
    try:
        if x.split(';')[0]!='User-ID' and x.split(';')[1]!='Location':
            if x.split(';')[1].split(',')[2].lstrip()!='':
                return (x.split(';')[0], x.split(';')[1].split(',')[2].lstrip())
    except:
        pass
dataUsers=rddUsers.map(func1)
dataUsers=dataUsers.filter(lambda x: x!=None)
#
def func2(x):
    try:
        if x.split(';')[0]!='User-ID' and x.split(';')[2]!='Book-Rating':
            return (x.split(';')[0], int(x.split(';')[2]))
    except:
        pass
dataRatings=rddRatings.map(func2)
dataRatings=dataRatings.filter(lambda x: x!=None)
#
data=dataUsers.join(dataRatings)
datakv=data.map(lambda x: (x[1][0],x[1][1]))
datakvG=datakv.groupByKey().mapValues(list)
dataFinal=datakvG.map(lambda x: '{};{};{:.1f}'.format(x[0],len(x[1]),sum(x[1])/len(x[1])))
#
dataFinal.saveAsTextFile("hdfs:///user/group10/bookstats")
