from pyspark import SparkContext,SparkConf

if __name__ == '__main__':
    conf = SparkConf().setAppName("appName").setMaster("local")
    sc = SparkContext(conf=conf)

    parallelize_rdd = sc.parallelize([1, 2, 3, 3, 4], 2)
    print ("parallelize_rdd = {0}".format(parallelize_rdd.glom().collect()))

    """
    第一个参数为withReplacement
    如果withReplacement=true的话表示有放回的抽样，采用泊松抽样算法实现
    如果withReplacement=false的话表示无放回的抽样，采用伯努利抽样算法实现

    第二个参数为：fraction，表示每一个元素被抽取为样本的概率，并不是表示需要抽取的数据量的因子
    比如从100个数据中抽样，fraction=0.2，并不是表示需要抽取100 * 0.2 = 20个数据，
    而是表示100个元素的被抽取为样本概率为0.2;样本的大小并不是固定的，而是服从二项分布
    当withReplacement=true的时候fraction>=0
    当withReplacement=false的时候 0 < fraction < 1

    第三个参数为：reed表示生成随机数的种子，即根据这个reed为rdd的每一个分区生成一个随机种子
    """
    sample_rdd = parallelize_rdd.sample(False, 0.5, 100)

    """
    结果：[[1], [3, 4]]
    """
    print ("sample_rdd = {0}".format(sample_rdd.glom().collect()))

    #标准差
    sample_stdev_rdd = sample_rdd.sampleStdev()
    sample_variance_rdd = parallelize_rdd.sampleVariance()

    """
    按照权重对RDD进行随机抽样切分，有几个权重就切分成几个RDD
    随机抽样采用伯努利抽样算法实现
    """
    split_rdds = parallelize_rdd.randomSplit([0.2, 0.8])

    print(len(split_rdds))
    print("split_rdds[0] = {0}".format(split_rdds[0].glom().collect()))
    print("split_rdds[1] = {0}".format(split_rdds[1].glom().collect()))

    """随机抽样指定数量的样本数据
    结果：[1]
    """
    print(parallelize_rdd.takeSample(False, 1))

    """分层采样"""
    pair_rdd = sc.parallelize([('A', 1), ('B', 2), ('C', 3), ('B', 4), ('A', 5)])
    sampleByKey_rdd = pair_rdd.sampleByKey(withReplacement=False,fractions={'A':0.5, 'B':1, 'C':0.2})
    """
    结果：[[('A', 1), ('B', 2), ('B', 4)]]
    """
    print("sampleByKey_rdd = {0}".format(sampleByKey_rdd.glom().collect()))