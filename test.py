from pyspark import SparkContext
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
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

    predictions3 = model3.transform(data)
    metrics3 = MulticlassMetrics(predictions3.select("prediction", "label").rdd)
    print("Accuracy for Model_3: " + str(metrics3.accuracy))


if __name__ == '__main__':
    sc = SparkContext(master="local[2]", appName="streamtest")
    ssc = StreamingContext(sc, 5)

    lines = ssc.socketTextStream("localhost", 6100)

    model3 = PipelineModel.load("log_model")


    lines.foreachRDD(rddstream)

    ssc.start()
    ssc.awaitTermination()