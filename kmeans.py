import os 
import pickle
import numpy as np
from sklearn import cluster
from sklearn.pipeline import make_pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix, classification_report, accuracy_score
from sklearn.cluster import MiniBatchKMeans
from pyspark import SparkContext
from pyspark.sql import SQLContext, functions
from pyspark.streaming import StreamingContext
from sklearn.metrics.pairwise import euclidean_distances
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover

import json

global center
center = []

global stop
stop = -3

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

    splitCol = functions.split(data['subject'], " ")
    data = data.withColumn('sub_words', splitCol)

    tokenizer = Tokenizer(inputCol="message", outputCol="words")
    wordsData = tokenizer.transform(data)

    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    cleanedData = remover.transform(wordsData)

    hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=5)
    featurizedData = hashingTF.transform(cleanedData)

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)

    X = np.array(rescaledData.select("features").collect())
    X = np.array([x[0] for x in X])
    Y = np.array(data.select("ham/spam").collect())
    Y = np.array([y[0] for y in Y])

    global stop
    if stop != 0:
        global center
        C = center.copy()
        kmeans.partial_fit(X)
        newcenter = kmeans.cluster_centers_
        if len(center) :
            
            dist =[]
            for i in range(len(C)):
                d = 0
                for j in range(len(C[i])):
                    d = d +((C[i][j]-newcenter[i][j])**2)
                dist.append(d**0.5)
            print(dist)
            if (dist[0]<0.01 and dist[1]<0.01):
                stop = stop +  1
            else: 
                stop = -3
        center = newcenter
        with open("kmean.pkl", "wb") as f:
            pickle.dump(kmeans, f)
    else:
        print("stopped")
        print(kmeans.cluster_centers_)
        exit()



if __name__ == '__main__':
    sc = SparkContext(master="local[2]", appName="streamtest")
    ssc = StreamingContext(sc, 5)

    kmeans = MiniBatchKMeans(n_clusters=2, reassignment_ratio=0.01)
    lines = ssc.socketTextStream("localhost", 6100)
 
    lines.map( lambda x : x.split('\n'))

    lines.foreachRDD(rddstream)

    ssc.start()
    ssc.awaitTermination()