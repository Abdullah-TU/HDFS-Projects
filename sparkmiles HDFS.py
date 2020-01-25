from pyspark.sql import SparkSession
spark = SparkSession\
        .builder\
        .appName("sparkmiles")\
        .getOrCreate()
sc = spark.sparkContext
#
df=spark.read.csv('/bigdata/vehicles/vehsample.csv', header=True, sep=';')
cars=df.filter((df['vehicleClass']=='M1') | (df['vehicleClass']=='M1G'))
data=cars.select(['deploymentDate', 'make', 'mileage(km)'])
data=data.withColumnRenamed('deploymentDate', 'date')
data=data.withColumnRenamed('mileage(km)', 'mileage')
#
makeRDD=data.select('make').rdd
makeRDDkv=makeRDD.map(list).map(lambda x: (x[0],1))
makeTops=makeRDDkv.reduceByKey(lambda a,b:a+b)
#
makeTops=makeTops.map(lambda x: (int(x[1]),x[0]))
makeTop5=makeTops.sortByKey(False).values().take(5)
#
newdata=data.withColumn('mileage', data['mileage'].cast('float'))
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
udfFunc=udf(lambda x:x.split('-')[0], StringType())
newdata1=newdata.withColumn('date', udfFunc(newdata.date))
#
pivot=newdata1.groupBy('make').pivot('date').mean('mileage')
pivotTop5=pivot.filter(pivot['make'].isin(makeTop5))
pdDFtop5=pivotTop5.toPandas()
#
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
pdDFtop5.loc[:,'1980':].T.dropna().plot()
plt.legend(list(pdDFtop5['make']))
plt.xlabel('date')
plt.savefig('miles.png')
