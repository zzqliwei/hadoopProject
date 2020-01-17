from pyspark import SparkContext,SparkConf

import time

def get_init_number(source):
    print("get init number from {0}, may be take much time........".format(source))
    time.sleep(1)
    return 1

if __name__ == '__main__':
    conf = SparkConf().setMaster("local").setAppName("appName")
    sc = SparkContext(conf=conf)

    parallelize_rdd = sc.parallelize([1, 2, 4, 3, 3, 6, 12], 2)

    """
    结果：[[1, 2, 4], [3, 3, 6,12]]
    """
    print("parallelize_rdd = {0}".format(parallelize_rdd.glom().collect()))


    """
    结果：[1, 2]
    """
    print("take(2) = {0}".format(parallelize_rdd.take(2)))
    """
    结果：[12, 6]
    """
    print("top(2) = {0}".format(parallelize_rdd.top(2)))

    """
    结果：[6, 4]
    """
    print ("top(2, key=str) = {0}".format(parallelize_rdd.top(2, key=str)))

    """
    top的第二个参数是一个函数，一般是lambda表达式，可以看下面python内置的max的例子
    当设置了这个key参数的时候，表示将数组中的元素应用这个key函数，按照返回的值来进行排序或者取最大值
    """

    d1 = {'name': 'egon', 'price': 100}
    d2 = {'name': 'rdw', 'price': 666}
    d3 = {'name': 'zat', 'price': 1}
    l1 = [d1, d2, d3]

    """
    表示取数组 l1 中name最大的那个元素的值
    """
    a = max(l1,key=lambda x:x['name'])
    """
    结果是：{'price': 1, 'name': 'zat'}
    """

    print(a)
    """
    表示取数组 l1 中price最大的那个元素的值
    """
    b = max(l1, key=lambda x: x['price'])
    """
    结果是：{'price': 666, 'name': 'rdw'}
    """
    print(b)

    """
    结果：first = 1
    """
    print("first = {0}".format(parallelize_rdd.first()))

    """
    结果：min = 1
    """
    print("min = {0}".format(parallelize_rdd.min(key=lambda x: str(x))))

    """
    结果：max = 12
    """
    print("max = {0}".format(parallelize_rdd.max()))

    """
    结果：takeOrdered = [1, 12]
    """
    print("takeOrdered = {0}".format(parallelize_rdd.takeOrdered(2,key=str)))

    def foreach_fun(x):
        init_number = get_init_number("foreach")
        print(str(x) + str(init_number) + "===========")

    parallelize_rdd.foreach(foreach_fun)

    def foreach_partition_fun(values):
        init_number = get_init_number("foreach")
        """
         和foreach api的功能是一样，只不过一个是将函数应用到每一条记录，这个是将函数应用到每一个partition
         如果有一个比较耗时的操作，只需要每一分区执行一次这个操作就行，则用这个函数
         这个耗时的操作可以是连接数据库等操作，不需要计算每一条时候去连接数据库，一个分区只需连接一次就行
         :param values:
         :return:
         """
        for item in values:
            print(str(item) + str(init_number) + "===========")
    parallelize_rdd.foreachPartition(foreach_partition_fun)

    reduce_result = parallelize_rdd.reduce(lambda a, b : a+b)
    print("reduce_result = {0}".format(reduce_result))

    tree_reduce_result = parallelize_rdd.treeReduce(lambda a, b : a+b ,2)
    print("tree_reduce_result = {0}".format(tree_reduce_result))

    def map_partition_with_index_func(partition_index, iterator): yield (partition_index, sum(iterator))
    map_partition_with_index_rdd = parallelize_rdd.mapPartitionsWithIndex(map_partition_with_index_func)
    """
        结果：[[(0, 3)], [(1, 10)]]
        """
    print("map_partition_with_index_rdd = {0}".format(map_partition_with_index_rdd.glom().collect()))

    print(parallelize_rdd.getNumPartitions())

    distinct_rdd = parallelize_rdd.distinct()
    print("distinct_rdd = {0}".format(distinct_rdd.glom().collect()))






