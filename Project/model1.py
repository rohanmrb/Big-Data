from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.classification import LinearSVC

import json


def rddstream(rdd):
    rdd1 = rdd.flatMap(lambda x: (json.loads(x)).items())
    rdd2 = (rdd1.map(lambda x: (x[0], tuple(x[1].values()))))
    rdd2 = (rdd2.map(lambda x: (int(x[0]), str(x[1][0]), str(x[1][1]), str(x[1][2]))))
    rdd3 = (rdd2.map(lambda x: (x[0], x[1], x[2], x[3], 0.0) if (str(x[3]) == "spam") else (x[0], x[1], x[2], x[3], 1.0)))
    rdd_lis = rdd3.collect()
    if(rdd_lis == [] or rdd_lis is None or rdd_lis == [[]]):
        return

    park_context = SQLContext(sc)
    data = park_context.createDataFrame(rdd_lis, ["row_no", "subject", "message", "ham/spam", "label"]).dropna('any')


    df, test_data = data.randomSplit([0.70, 0.30], 1234)

    model1 = pipeline.fit(df)

    predictions = model1.transform(test_data)
    print(predictions.select("label", "prediction").show(10000))

sc = SparkContext(master="local[2]", appName="streamtest")
ssc = StreamingContext(sc, 5)

lines = ssc.socketTextStream("localhost", 6100)

tokenizer = Tokenizer(inputCol="message", outputCol="words")
hashingTF = HashingTF(
    inputCol="words", outputCol="rawFeatures")
idf = IDF(inputCol="rawFeatures", outputCol="features")
svm = LinearSVC(featuresCol='features', labelCol='label',
                predictionCol='prediction')
pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, svm])
# pipeline.write().overwrite().save("./pipeline")

lines.foreachRDD(rddstream)

ssc.start()
ssc.awaitTermination()
