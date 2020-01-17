from pyspark import SparkContext, SparkConf

import os
import shutil

if __name__ == '__main__':
    conf = SparkConf().setAppName("appName").setMaster("local")
    sc = SparkContext(conf=conf)

    sourceDataRDD = sc.textFile("E:/IDEAProject/hadoopProject/spark-parctise/9-spark-dataset/src/main/resources/环境安装.txt")

    wordsRDD = sourceDataRDD.flatMap(lambda line: line.split())

    keyValueWordsRDD = sourceDataRDD.map(lambda word: (word,1))

    wordCountRDD =  keyValueWordsRDD.reduceByKey(lambda a, b: a + b)

    outputPath = "E:/IDEAProject/hadoopProject/spark-parctise/9-spark-dataset/src/main/resources/wordcountPyth"
    if os.path.exists(outputPath):
        shutil.rmtree(outputPath)

    wordCountRDD.saveAsTextFile(outputPath)

    print(wordCountRDD.collect())