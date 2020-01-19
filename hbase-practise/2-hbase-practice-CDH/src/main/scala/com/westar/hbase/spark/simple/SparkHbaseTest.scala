package com.westar.hbase.spark.simple

import com.westar.hbase.spark.HBaseConnectionCache
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

object SparkHbaseTest {

  def doWithHbaseOnDriver(sc: SparkContext) = {
    val conf = HBaseConfiguration.create(sc.hadoopConfiguration)
    val connection = ConnectionFactory.createConnection(conf)
    //可以做一些Admin可以做的事情，比如表的创建等。
    val admin = connection.getAdmin
    val tableName = TableName.valueOf("webtable")
    if(!admin.tableExists(tableName)){
      val table = new HTableDescriptor(tableName)
      table.addFamily(new HColumnDescriptor("content"))
      table.addFamily(new HColumnDescriptor("language"))
      admin.createTable(table)
    }
    //当然也可以对表进行增删改查(但是对于大量的数据不建议在driver端进行)

  }

  def putData2Hbase(sc: SparkContext): Unit = {
    val webContent = sc.textFile("hdfs://.....").map(line =>{
      val token = line.split(" ")
      (token(0),token(1))
    })

    webContent.foreachPartition{ it =>
      val conf = HBaseConfiguration.create(sc.hadoopConfiguration)
      val connection = HBaseConnectionCache.getConnection(conf)
      val tableName = TableName.valueOf("webtable")

      val table = connection.getTable(tableName)
      val puts = it.map {case (rowkey, content) =>
        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(Bytes.toBytes("content"), null, Bytes.toBytes(content))
        put
      }.toSeq

      import scala.collection.JavaConversions.seqAsJavaList
      table.put(puts)

      table.close()
      connection.close()
    }
  }

  def getDataFromHbase(sc: SparkContext) = {
    val urlRDD = sc.textFile("HDFS://....")
    urlRDD.mapPartitions{ it =>
      val conf = HBaseConfiguration.create(sc.hadoopConfiguration)
      val connection = ConnectionFactory.createConnection(conf)
      val tableName = TableName.valueOf("webtable")

      val table = connection.getTable(tableName)
      it.map { rowKey =>
        val get = new Get(Bytes.toBytes(rowKey))
        val result = table.get(get)
        (rowKey, Bytes.toString(result.getValue(Bytes.toBytes("language"), null)))
      }
    }.saveAsTextFile("HDFS://....")
  }

  def cloneSnapshot(sc: SparkContext): Unit = {
    val conf = HBaseConfiguration.create(sc.hadoopConfiguration)

    val connection = ConnectionFactory.createConnection(conf)

    //可以做一些Admin可以做的事情，比如表的创建等。
    val admin = connection.getAdmin
    val tableName = TableName.valueOf("webtable")
    admin.disableTable(tableName)
    admin.cloneSnapshot("webtable-20180409", tableName)
    admin.enableTable(tableName)
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("SparkHbaseTest")

    val sc = new SparkContext(sparkConf)

    doWithHbaseOnDriver(sc)

    //对于大量的数据，我们需要放到每一个executor上去并行的执行
    //1、将文件中的web content的数据都插入到Hbase中
    putData2Hbase(sc)

    //2、在每一个Task中去get需要的数据
    getDataFromHbase(sc)


    //在driver端还可以为hbase表创建snapshot
    cloneSnapshot(sc)

  }

}
