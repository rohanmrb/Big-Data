from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.classification import LinearSVC, NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

import json


def rddstream(rdd):
    rdd1 = rdd.flatMap(lambda x: (json.loads(x)).items())
    rdd2 = (rdd1.map(lambda x: (x[0], tuple(x[1].values()))))
    rdd2 = (rdd2.map(lambda x: (int(x[0]), str(
        x[1][0]), str(x[1][1]), str(x[1][2]))))
    rdd3 = (rdd2.map(lambda x: (x[0], x[1], x[2], x[3], 1.0) if (
        str(x[3]) == "spam") else (x[0], x[1], x[2], x[3], 0.0)))
    rdd_lis = rdd3.collect()
    if(rdd_lis == [] or rdd_lis is None or rdd_lis == [[]]):
        return

    park_context = SQLContext(sc)
    data = park_context.createDataFrame(
        rdd_lis, ["row_no", "subject", "message", "ham/spam", "label"]).dropna('any')

    train_data, test_data = data.randomSplit([0.70, 0.30], 1234)

    model1 = pipeline.fit(train_data)
    predictions1 = model1.transform(test_data)

    model2 = naive_pipe.fit(train_data)
    predictions2 = model2.transform(test_data)

    print(predictions2.select("label", "prediction").show(1000))


if __name__ == '__main__':
    sc = SparkContext(master="local[2]", appName="streamtest")
    ssc = StreamingContext(sc, 5)

    lines = ssc.socketTextStream("localhost", 6100)

    # performing Tf-idf to obtain features of messages
    tokenizer = Tokenizer(inputCol="message", outputCol="words")
    hashingTF = HashingTF(
        inputCol="words", outputCol="rawFeatures")
    idf = IDF(inputCol="rawFeatures", outputCol="features")

    # SVM model pipeline
    svm = LinearSVC(featuresCol='features', labelCol='label',
                    predictionCol='prediction')
    pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, svm])

    # naviebayes pipeline
    naivebayes = NaiveBayes(featuresCol="features", labelCol="label",
                            predictionCol="prediction", smoothing=1.0, modelType="multinomial")
    naive_pipe = Pipeline(stages=[tokenizer, hashingTF, idf, naivebayes])

    lines.foreachRDD(rddstream)

    ssc.start()
    ssc.awaitTermination()
