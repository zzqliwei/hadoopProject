package com.westar.hbase.spark

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{IdentityTableMapper, TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SerializableWritable, SparkContext}

import scala.reflect.ClassTag

/**
  * HBaseContext是Spark操作Hbase的facade类
  * 包含了get,put,delete,mutate,scan等操作
  */
class HBaseContext(@transient sc: SparkContext
                   ,@transient val config: Configuration)
  extends Serializable with Logging{

  @transient val job = Job.getInstance(config)

  //因为配置文件比较小，每一个Executor又必须要，所以通过broadcast的方式广播到每一个executor上
  val broadcastedConf = sc.broadcast(new SerializableWritable(config))

  LatestHBaseContextCache.latest = this

  /**
    * 对原生的Spark RDD的foreachPartition方法操作的一个丰富，需要传入准备好的Hbase Connection
    *
    * Note: 在这个方法中不能关闭连接，这个连接是调用这个方法的客户端来管理的
    *
    * @param rdd 需要迭代遍历的原生RDD
    * @param f   迭代原生RDD并且和HBase交互的foreach函数
    */

  def foreachPartition[T](rdd:RDD[T],f:(Iterator[T], Connection) => Unit) : Unit ={
    rdd.foreachPartition(
      it => hbaseForeachPartition(broadcastedConf ,it, f)
    )
  }
  def hbaseForeachPartition[T](configBroadcast:Broadcast[SerializableWritable[Configuration]],
                            it:Iterator[T], foreachPartition:(Iterator[T], Connection) => Unit) = {
    //拿到需要连接HBase的配置信息
    val config = getConf(configBroadcast)
    // 获取一个HBase连接，如果已经存在则拿已经存在的连接，保证每一个Executor只有一个连接
    val smartConn = HBaseConnectionCache.getConnection(config)

    //  真正执行与HBase交互的方法逻辑
    foreachPartition(it, smartConn.connection)
    smartConn.close()
  }

  /**
    * 对原生的Spark RDD的mapPartition方法操作的一个丰富，需要传入准备好的Hbase Connection
    *
    * Note: 在这个方法中不能关闭连接，这个连接是调用这个方法的客户端来管理的
    *
    * @param rdd 需要迭代的原生RDD
    * @param mp 迭代原生RDD并且和HBase交互的map函数
    * @return  返回一个mapPartitions返回新的RDD
    */
  def mapPartitions[T, R: ClassTag](rdd: RDD[T],
                                    mp: (Iterator[T], Connection) => Iterator[R]): RDD[R] = {

    rdd.mapPartitions[R](it => hbaseMapPartition[T, R](broadcastedConf, it, mp))
  }
  /**
    * 获取HBase连接，并且将mapPartition函数应用到RDD的每一个分区数据上
    */
  private def hbaseMapPartition[K, U](configBroadcast: Broadcast[SerializableWritable[Configuration]],
                                      it: Iterator[K],
                                      mapPartition: (Iterator[K], Connection) => Iterator[U]): Iterator[U] = {

    val config = getConf(configBroadcast)

    val smartConn = HBaseConnectionCache.getConnection(config)
    val res = mapPartition(it, smartConn.connection)
    smartConn.close()
    res
  }


  //从广播变量中获取配置信息
  private def getConf(configBroadcast: Broadcast[SerializableWritable[Configuration]]): Configuration = configBroadcast.value.value

  /**
    * 对HBaseContext.foreachPartition方法的一个简单抽象
    * 支持调用方传入一个原生RDD，并且可以根据这个RDD的值产生对应的put操作发往HBase
    * 使得客户端不需要关系HBase的连接是怎么创建的，降低了客户端的难度
    *
    * @param rdd       需要迭代的原生RDD
    * @param tableName 需要插入数据的HBase表名
    * @param f         将RDD中的值转化为Put的函数
    */
  def bulkPut[T](rdd:RDD[T], tableName: TableName, f: (T) => Put) {
    val tName = tableName.getName
    rdd.foreachPartition(
      it =>hbaseForeachPartition[T](
        broadcastedConf,
        it,
        (iterator, connection) => {
          val m = connection.getBufferedMutator(TableName.valueOf(tName))
          iterator.foreach(T => m.mutate(f(T)))
          m.flush()
          m.close()
        })
    )
  }

  /**
    * A simple abstraction over the HBaseContext.mapPartition method.
    * HBaseContext.mapPartition方法的简单抽象
    *
    * 允许用户传入一个RDD，和这个RDD值转化成get的函数，并且将这些get请求发送到HBase中
    * 返回一个新的RDD
    *
    *
    * @param rdd           需要迭代的原生RDD
    * @param tableName     获取数据的Hbase表
    * @param batchSize      批量请求Hbase的大小
    * @param makeGet       根据RDD值转化为Get请求的函数
    * @param convertResult  根据从HBase中拿到的get的结果转化为客户想要的类型的值
    *
    * @return 返回一个新的类型的RDD
    */
  def bulkGet[T, U: ClassTag](tableName: TableName,
                              batchSize: Integer,
                              rdd: RDD[T],
                              makeGet: (T) => Get,
                              convertResult: (Result) => U): RDD[U] = {

    val getMapPartition = new GetMapPartition[T, U](tableName,
      batchSize,
      makeGet,
      convertResult)

    rdd.mapPartitions[U](it =>
      hbaseMapPartition[T, U](
        broadcastedConf,
        it,
        getMapPartition.run))
  }

  private class GetMapPartition[T, U](tableName: TableName,
                                      batchSize: Integer,
                                      makeGet: (T) => Get,
                                      convertResult: (Result) => U)
    extends Serializable {
    val tName = tableName.getName
    def run(iterator: Iterator[T], connection: Connection):Iterator[U] ={
      val table = connection.getTable(TableName.valueOf(tName))

      val gets = new java.util.ArrayList[Get]()
      var res = List[U]()

      while (iterator.hasNext) {
        gets.add(makeGet(iterator.next()))

        if (gets.size() == batchSize) {
          val results = table.get(gets)
          res = res ++ results.map(convertResult)
          gets.clear()
        }
      }
      if (gets.size() > 0) {
        val results = table.get(gets)
        res = res ++ results.map(convertResult)
        gets.clear()
      }
      table.close()
      res.iterator
    }
  }

  /**
    * bulkMutation的实现
    */
  private def bulkMutation[T](rdd: RDD[T], tableName: TableName,
                              f: (T) => Mutation, batchSize: Integer) {

    val tName = tableName.getName
    rdd.foreachPartition(
      it => hbaseForeachPartition[T](
        broadcastedConf,
        it,
        (iterator, connection) => {
          val table = connection.getTable(TableName.valueOf(tName))
          val mutationList = new java.util.ArrayList[Mutation]
          iterator.foreach(T => {
            mutationList.add(f(T))
            if (mutationList.size >= batchSize) {
              table.batch(mutationList, null)
              mutationList.clear()
            }
          })
          if (mutationList.size() > 0) {
            table.batch(mutationList, null)
            mutationList.clear()
          }
          table.close()
        }))
  }

  /**
    * bulkDelete的实现
    */
  def bulkDelete[T](rdd: RDD[T], tableName: TableName,
                    f: (T) => Delete, batchSize: Integer) {
    bulkMutation(rdd, tableName, f, batchSize)
  }


  /**
    *   Spark 对 Hbase进行Scan的实现
    * @param tableName 需要Scan的表名
    * @param scan      scan对象
    * @param f         将scan到的结果数据转化为用户想要的数据的函数
    * @return 新的类型的RDD
    */
  def hbaseRDD[U: ClassTag](tableName: TableName, scan: Scan,
                            f: ((ImmutableBytesWritable, Result)) => U): RDD[U] = {
    val job: Job = Job.getInstance(getConf(broadcastedConf))
    TableMapReduceUtil.initCredentials(job)
    TableMapReduceUtil.initTableMapperJob(tableName, scan,
      classOf[IdentityTableMapper], null, null, job)

    sc.newAPIHadoopRDD(job.getConfiguration,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]).map(f)
  }

  /**
    *   Spark 对 Hbase进行Scan的实现
    *   hbaseRDD方法的重载方法，返回的结果就是scan结果类型的RDD
    * @param tableName 需要Scan的表名
    * @param scan      scan对象
    * @return Scan出结果类型的RDD
    */
  def hbaseRDD(tableName: TableName, scan: Scan): RDD[(ImmutableBytesWritable, Result)] = {

    hbaseRDD[(ImmutableBytesWritable, Result)](
      tableName,
      scan,
      (r: (ImmutableBytesWritable, Result)) => r)
  }


  def streamForeachPartition[T](dstream: DStream[T],
                                f: (Iterator[T], Connection) => Unit): Unit = {

    dstream.foreachRDD(rdd => this.foreachPartition(rdd, f))
  }

  def streamMapPartitions[T, U: ClassTag](dstream: DStream[T],
                                          f: (Iterator[T], Connection) => Iterator[U]):
  DStream[U] = {
    dstream.mapPartitions(it => hbaseMapPartition[T, U](
      broadcastedConf,
      it,
      f))
  }

  def streamBulkPut[T](dstream: DStream[T],
                       tableName: TableName,
                       f: (T) => Put) = {
    val tName = tableName.getName
    dstream.foreachRDD((rdd, time) => {
      bulkPut(rdd, TableName.valueOf(tName), f)
    })
  }

  def streamBulkDelete[T](dstream: DStream[T],
                          tableName: TableName,
                          f: (T) => Delete,
                          batchSize: Integer) = {
    streamBulkMutation(dstream, tableName, f, batchSize)
  }

  private def streamBulkMutation[T](dstream: DStream[T],
                                    tableName: TableName,
                                    f: (T) => Mutation,
                                    batchSize: Integer) = {
    val tName = tableName.getName
    dstream.foreachRDD((rdd, time) => {
      bulkMutation(rdd, TableName.valueOf(tName), f, batchSize)
    })
  }

  def streamBulkGet[T, U: ClassTag](tableName: TableName,
                                    batchSize: Integer,
                                    dStream: DStream[T],
                                    makeGet: (T) => Get,
                                    convertResult: (Result) => U): DStream[U] = {

    val getMapPartition = new GetMapPartition[T, U](tableName,
      batchSize,
      makeGet,
      convertResult)

    dStream.mapPartitions[U](it => hbaseMapPartition[T, U](
      broadcastedConf,
      it,
      getMapPartition.run))
  }

}

object LatestHBaseContextCache {
  var latest: HBaseContext = null
}
