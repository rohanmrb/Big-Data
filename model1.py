from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, Word2Vec
from pyspark.ml.classification import LinearSVC, NaiveBayes, LogisticRegression
from pyspark.ml.regression import LinearRegression
from pyspark.mllib.evaluation import MulticlassMetrics

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

    #model1 = pipeline.fit(train_data)
    #predictions1 = model1.transform(test_data)

    #model2 = naive_pipe.fit(train_data)
    #predictions2 = model2.transform(test_data)

    model3 = logReg_pipeline.fit(train_data)
    model3.write().overwrite().save('log_model')
    # predictions3 = model3.transform(test_data)


    #metrics1 = MulticlassMetrics(predictions1.select("prediction", "label").rdd)
    #print("Accuracy for Model_1: " + str(metrics1.accuracy))
    print("Confusion Matrix for Model_1: ")
    #metrics1.confusionMatrix().show()

    metrics3 = MulticlassMetrics(predictions3.select("prediction", "label").rdd)
    print("Accuracy for Model_3: " + str(metrics3.accuracy))

    # print(predictions2.select("label", "prediction").show(1000))


if __name__ == '__main__':
    sc = SparkContext(master="local[2]", appName="streamtest")
    ssc = StreamingContext(sc, 5)

    lines = ssc.socketTextStream("localhost", 6100)

    # performing Word2Vec to obtain the vector representation of the words
    word2Vec = Word2Vec(inputCol="message", outputCol="features")

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
    #naivebayes = NaiveBayes(featuresCol="features", labelCol="label",
    #                        predictionCol="prediction", smoothing=1.0, modelType="multinomial")
    #naive_pipe = Pipeline(stages=[word2Vec, naivebayes])

    # linear regression
    #lr = LinearRegression(featuresCol="features", labelCol="label", predictionCol="prediction")
    #lr_pipeline = Pipeline(stages=[word2Vec, lr])

    # logistic regression
    logReg = LogisticRegression(featuresCol="features", labelCol="label", predictionCol="prediction")
    logReg_pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, logReg])

    lines.foreachRDD(rddstream)

    ssc.start()
    ssc.awaitTermination()
