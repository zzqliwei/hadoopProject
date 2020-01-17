#coding:utf-8
from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    conf = SparkConf().setAppName("appName").setMaster("local")

    sc = SparkContext(conf=conf)

    """
    创建RDD的方法:
    1: 从一个稳定的存储系统中，比如hdfs文件, 或者本地文件系统
    """
    text_file_rdd = sc.textFile("E:/IDEAProject/hadoopProject/spark-parctise/9-spark-dataset/src/main/resources/环境安装.txt")
    print("text_file_rdd = {0}".format(",".join(text_file_rdd.collect())))

    """
    2: 从一个已经存在的RDD中, 即RDD的transformation api
    """
    map_rdd = text_file_rdd.map(lambda line : "{0}-{1}".format(line,"test"))
    print("map_rdd = {0}".format(",".join(map_rdd.collect())))

    """
    3: 从一个已经存在于内存中的列表,  可以指定分区，如果不指定的话分区数为所有executor的cores数
    """
    parallelize_rdd = sc.parallelize([1, 2, 3, 3, 4], 2)
    print("parallelize_rdd = {0}".format(parallelize_rdd.glom().collect()))