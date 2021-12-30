from pyspark import SparkContext
from pyspark.sql import SQLContext
import sys

spark_context = SparkContext.getOrCreate()
spark_context = SQLContext(spark_context)

df = spark_context.read.option("header",True).csv(sys.argv[1])
Gdf = spark_context.read.option("header",True).csv(sys.argv[2])

df1 = df.selectExpr("dt","cast(AverageTemperature as float) AverageTemperature","City","Country").na.drop('any')
Gdf1 = Gdf.selectExpr("dt","cast(LandAverageTemperature as float) LandAverageTemperature").na.drop('any')

df1_sorted = df1.groupBy("Country","dt").max("AverageTemperature")
abc = df1_sorted.join(Gdf1,df1_sorted.dt == Gdf1.dt,"inner")
abc = abc.rdd.map(lambda x:(x[0],1) if (x[2] > x[4]) else (x[0],0))

x = abc.toDF().sort("_1")
x = x.filter(x._2.isin('1'))
x = x.rdd.map(lambda x:(x[0],x[1]))

y = x.reduceByKey(lambda a,b :a+b).collect()
y.sort(key = lambda x:x[0])

for q,w in y:
    print(f'{q}\t{w}')

