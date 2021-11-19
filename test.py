
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.ml.feature import HashingTF, IDF, Tokenizer

import json


def rddstream(rdd):

    rdd1 = rdd.flatMap(lambda x: (json.loads(x)).items())

    rdd2 = (rdd1.map(lambda x: tuple(x[1].values())))

    rdd_lis = rdd2.collect()
    park_context = SQLContext(sc)
    data = park_context.createDataFrame(rdd_lis,["subject","message","ham/spam"])
    # print(data.show(100))



    tokenizer = Tokenizer(inputCol="message", outputCol="words")
    wordsData = tokenizer.transform(data)

    hashingTF = HashingTF(
        inputCol="words", outputCol="rawFeatures", numFeatures=20)
    featurizedData = hashingTF.transform(wordsData)
    # alternatively, CountVectorizer can also be used to get term frequency vectors

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)

    print(rescaledData.show(10000))


sc = SparkContext(master="local[2]", appName="streamtest")
ssc = StreamingContext(sc, 12)

lines = ssc.socketTextStream("localhost", 6100)

lines.foreachRDD(rddstream)

ssc.start()
ssc.awaitTermination()
