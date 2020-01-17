package com.westar.spark.rdd.sources

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat => NewTableInputFormat, TableOutputFormat => NewTableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.util.Options.ClassOption
import org.apache.spark.{SparkConf, SparkContext}

object HbaseFileFormatApiTest {

  def writeHbaseTableWithOldMRApi(sc: SparkContext, conf: Configuration, tableName: String): Unit = {
    //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val dataRDD = sc.makeRDD(Array("user-1,jeffy,15", "user-2,kkk,16", "user-3,ddd,16"))
    val rdd = dataRDD.map(_.split(",")).map{ arr =>
      val put = new Put(Bytes.toBytes(arr(0).toInt))
      put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("n"),Bytes.toBytes(arr(1)))
      put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("a"),Bytes.toBytes(arr(2).toInt))

      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable,put)
    }

    rdd.saveAsHadoopDataset(jobConf)
  }

  def writeHbaseTableWithNewMRApi(sc: SparkContext, tableName: String): Unit = {
    sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "slave1,slave2")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    sc.hadoopConfiguration.set(NewTableOutputFormat.OUTPUT_TABLE, tableName)

    val job = Job.getInstance(sc.hadoopConfiguration)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[NewTableOutputFormat[ImmutableBytesWritable]])

    val dataRDD = sc.makeRDD(Array("user-1,jeffy,15", "user-2,kkk,16", "user-3,ddd,16"))

    val rdd = dataRDD.map(_.split(',')).map { arr => {
      val put = new Put(Bytes.toBytes(arr(0)))
      put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("n"), Bytes.toBytes(arr(1)))
      put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("a"), Bytes.toBytes(arr(2).toInt))
      (new ImmutableBytesWritable, put)
    }
    }

    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())

  }

  def createTableIfNotExsit(conf: Configuration, tableName: String): Unit = {
    val connection = ConnectionFactory.createConnection(conf)
    val admin = connection.getAdmin
    if(!admin.isTableAvailable(TableName.valueOf(tableName))){
      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
      admin.createTable(tableDesc)
    }
    admin.close()
  }

  def readHbaseTableWithOldMRApi(sc: SparkContext, conf: Configuration, tableName: String): Unit = {
    val jobConf = new JobConf(conf)
    jobConf.setInputFormat(classOf[TableInputFormat])
    FileInputFormat.setInputPaths(jobConf, "/habse/path/to/user")

    // 如果表不存在则创建表
    createTableIfNotExsit(conf, tableName)

    //读取数据并转化成rdd
    val hBaseRDD = sc.hadoopRDD(jobConf,classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],classOf[Result])

    val count = hBaseRDD.count()
    println(count)
    hBaseRDD.foreach{ case (_,result: Result) =>
      //获取行键
      val key = Bytes.toString(result.getRow)
      //通过列族和列名获取列
      val name = Bytes.toString(result.getValue("f".getBytes,"n".getBytes))
      val age = Bytes.toInt(result.getValue("f".getBytes,"a".getBytes))
      println("Row key:" + key + " Name:" + name + " Age:" + age)
    }
  }

  def readHbaseTableWithNewMRApi(sc: SparkContext, conf: Configuration, tableName: String) = {
    conf.set(NewTableInputFormat.INPUT_TABLE, tableName)
    // 如果表不存在则创建表
    createTableIfNotExsit(conf, tableName)

    //读取数据并转化成rdd
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[NewTableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val count = hBaseRDD.count()

    println(count)

    hBaseRDD.foreach { case (_, result) =>
      //获取行键
      val key = Bytes.toString(result.getRow)
      //通过列族和列名获取列
      val name = Bytes.toString(result.getValue("f".getBytes, "n".getBytes))
      val age = Bytes.toInt(result.getValue("f".getBytes, "a".getBytes))
      println("Row key:" + key + " Name:" + name + " Age:" + age)
    }
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址
    conf.set("hbase.zookeeper.quorum", "slave1,slave2")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    val tableName = "user"

    writeHbaseTableWithOldMRApi(sc,conf,tableName)

    writeHbaseTableWithNewMRApi(sc, tableName)

    readHbaseTableWithOldMRApi(sc, conf, tableName)

    readHbaseTableWithNewMRApi(sc, conf, tableName)

    sc.stop()
  }

}
