from pyspark import SparkContext
from pyspark.sql import SQLContext
import sys

spark_context = SparkContext.getOrCreate()
spark_context = SQLContext(spark_context)
df = spark_context.read.option("header",True).csv(sys.argv[2])
df1 = df.selectExpr("dt","cast(AverageTemperature as float) AverageTemperature","City","Country").na.drop('any')
df1=df1[df1.Country.isin(sys.argv[1])]
df_avg = df1.groupBy("City").mean("AverageTemperature")
abc = df1.join(df_avg,df1.City==df_avg.City,"inner")
abc = abc.rdd.map(lambda x:(x[2],1) if (x[1]> x[5]) else (x[2],0))
x = abc.toDF().sort("_1")
x = x.filter(x._2.isin('1'))
x = x.rdd.map(lambda x:(x[0],x[1]))
y = x.reduceByKey(lambda a,b :a+b).collect()
y.sort(key = lambda x:x)
for q,w in y:
    print(f'{q}\t{w}')