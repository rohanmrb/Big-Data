import os 
import pickle
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix, accuracy_score
from pyspark import SparkContext
from pyspark.sql import SQLContext, functions
from pyspark.streaming import StreamingContext
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from sklearn.metrics import silhouette_score
from pyspark.ml.feature import StopWordsRemover
import json

def rddstream(rdd):

    rdd1 = rdd.flatMap(lambda x: (json.loads(x)).items())

    rdd2 = (rdd1.map(lambda x: (x[0], tuple(x[1].values()))))

    rdd2 = (rdd2.map(lambda x: (int(x[0]), str(

        x[1][0]), str(x[1][1]).lower(), str(x[1][2]))))

    rdd3 = (rdd2.map(lambda x: (x[0], x[1], x[2], x[3], 1.0) if (

        str(x[3]) == "spam") else (x[0], x[1], x[2], x[3], 0.0)))

    rdd_lis = rdd3.collect()

    if(rdd_lis == [] or rdd_lis is None or rdd_lis == [[]]):
        return

    park_context = SQLContext(sc)
    data = park_context.createDataFrame(

        rdd_lis, ["row_no", "subject", "message", "ham/spam", "label"]).dropna('any')

    tokenizer = Tokenizer(inputCol="message", outputCol="words")
    wordsData = tokenizer.transform(data)

    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    cleanedData = remover.transform(wordsData)

    hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=5)
    featurizedData = hashingTF.transform(cleanedData)

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)

    X_tfidf = np.array(rescaledData.select("features").collect())
    X_tfidf = np.array([x[0] for x in X_tfidf])



    Y = np.array(data.select("label").collect())
    Y = np.array([y[0] for y in Y])



    with open("kmean.pkl", "rb") as f:
        model = pickle.load(f)
    predictions = model.predict(X_tfidf)
    score = accuracy_score(Y,predictions)
    
    diff = silhouette_score(X_tfidf, model.labels_, metric='euclidean')
    print('Accuracy:{0:f}'.format(score))
    print('silhouette_score:{0:f}'.format(diff))


if __name__ == '__main__':

    sc = SparkContext(master="local[2]", appName="streamtest")

    ssc = StreamingContext(sc, 5)

    lines = ssc.socketTextStream("localhost", 6100)
    lines.map( lambda x : x.split('\n'))

    lines.foreachRDD(rddstream)
    ssc.start()

    ssc.awaitTermination()
