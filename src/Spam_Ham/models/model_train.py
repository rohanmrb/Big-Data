import os 
import pickle
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import BernoulliNB, MultinomialNB
from pyspark import SparkContext
from pyspark.sql import SQLContext, functions
from pyspark.streaming import StreamingContext
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import StopWordsRemover
from sklearn.linear_model import SGDClassifier, SGDRegressor
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

    hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=100)
    featurizedData = hashingTF.transform(cleanedData)

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)

    c_v = CountVectorizer(inputCol="words", outputCol="features", vocabSize=3, minDF=2.0)
    result = c_v.fit(wordsData)
    count_vectors = result.transform(wordsData)


    X_cv = np.array(count_vectors.select("features").collect())
    X_cv = np.array([x[0] for x in X_cv])
    
    X_tfidf = np.array(rescaledData.select("features").collect())
    X_tfidf = np.array([x[0] for x in X_tfidf])

    Y = np.array(data.select("ham/spam").collect())
    Y = np.array([y[0] for y in Y])


    M_NB = MultinomialNB(alpha=10 ** -4)
    B_NB = BernoulliNB(alpha=10**-5)

    SGD_clas = SGDClassifier(alpha=10**-4, epsilon=0.01)
    SGD_reg = SGDRegressor(alpha=10**-5)


    # Tranform_List = [X_cv,  X_tfidf]
    # name_list = ['cv', 'tfidf']
    Model_List = [M_NB, B_NB, SGD_clas]


    X = X_tfidf   #OR X_cv
    k = "tfidf"
 
    for j in Model_List:
            path = str(str(k)+'_'+str(j))
            
            if os.path.exists(path) == False:
                new_model = j
                model0 = new_model.partial_fit(X=X, y=Y, classes=["ham","spam"])
                with open(path, "wb") as f:
                    pickle.dump(model0,f)
            else:
                with open(path, "rb") as f:
                    model = pickle.load(f)
                model_1 = model.partial_fit(X=X,y=Y, classes=["ham","spam"])
                with open(path, "wb") as f:
                    pickle.dump(model, f)

    SGD_List = [SGD_reg]

    Y_label = np.array(data.select("label").collect())
    Y_label = np.array([y[0] for y in Y_label])
    
    for j in SGD_List:

            path = str(str(k)+'_'+str(j))
            
            if os.path.exists(path) == False:
                new_model = j
                model0 = new_model.partial_fit(X=X, y=Y_label)
                with open(path, "wb") as f:
                    pickle.dump(model0,f)                
            else:
                with open(path, "rb") as f:
                    model = pickle.load(f)
                model_1 = model.partial_fit(X=X,y=Y_label)
                with open(path, "wb") as f:
                    pickle.dump(model, f)
    print("________iteration_of_for_loop___________")
                

if __name__ == '__main__':
    sc = SparkContext(master="local[2]", appName="streamtest")
    ssc = StreamingContext(sc, 5)

    lines = ssc.socketTextStream("localhost", 6100)

    lines.map( lambda x : x.split('\n'))

    lines.foreachRDD(rddstream)

    ssc.start()
    ssc.awaitTermination()
