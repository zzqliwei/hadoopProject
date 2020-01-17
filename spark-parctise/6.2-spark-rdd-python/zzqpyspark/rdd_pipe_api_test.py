from pyspark import SparkContext,SparkConf
if __name__ == '__main__':
    conf = SparkConf().setAppName("appName").setMaster("local")
    sc = SparkContext(conf=conf)

    parallelize_rdd = sc.parallelize(["test1", "test2", "test3", "test4", "test5"], 2)
    pipe_rdd = parallelize_rdd.pipe("python /Users/tangweiqun/spark/source/spark-course/spark-rdd-java/src/main/resources/echo.py",{"env":"env"})
    """
    结果：slave1-test1-env slave1-test2-env slave1-test3-env slave1-test4-env slave1-test5-env
    """
    print("pipe_rdd = {0}".format(" ".join(pipe_rdd.collect())))