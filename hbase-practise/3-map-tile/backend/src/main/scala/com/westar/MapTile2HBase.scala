package com.westar

import java.util

import no.ecc.vectortile.VectorTileEncoder
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/*
export HADOOP_CONF_DIR=/home/hadoop-twq/bigdata/hadoop-2.7.5/etc/hadoop
spark-submit --class com.twq.MapTile2HBase \
--master yarn \
--executor-memory 512M \
--executor-cores 2 \
--num-executors 2 \
--conf spark.maptile.inputPath=hdfs://master:9999/user/hadoop-twq/maptile/hz_building.csv \
--conf spark.maptile.levels=0,16 \
--conf spark.maptile.partitionsPerLevelStr=1,1,1,1,1,1,1,1,1,1,1,1,2,3,5,6,6 \
--conf spark.maptile.hbase.tableName=maptile \
--conf spark.maptile.hbase.zk=master,slave1,slave2 \
--conf spark.executor.extraJavaOptions="-Dcom.sun.management.jmxremote.port=1099 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false " \
/home/hadoop-twq/maptile/backend-1.0-SNAPSHOT-jar-with-dependencies.jar
 */
object MapTile2HBase {
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val conf = new SparkConf()
    val isLocal = !conf.contains("spark.master")
    if (isLocal) {
      conf.setMaster("local[6]") // 指定local方式的话，其实就是在本地启动一个executor，这个executor只有一个core
    }

    val spark = SparkSession.builder()
      .appName("MapTile")
      .config(conf)
      .getOrCreate()

    val (inputPath, levels, partitionsPerLevelStr, tableName, zk) = if (isLocal) {
      ("backend/src/main/resources/hz_building.csv",
        "0,16",
        "1,1,1,1,1,1,1,1,1,1,1,1,2,3,5,6,6",
        "maptile",
        "master,slave1,slave2")
    } else {
      (conf.get("spark.maptile.inputPath"),
        conf.get("spark.maptile.levels"),
        conf.get("spark.maptile.partitionsPerLevelStr"),
        conf.get("spark.maptile.hbase.tableName"),
        conf.get("spark.maptile.hbase.zk"))
    }
    //1、读取数据源
    val rawDataRDD = spark.sparkContext.textFile(inputPath)
    // rawDataRDD有几个分区
    // 如果是local方式的，那么这个分区数是1
    // 如果是local[n] (n > 1)：那么这个分区数是2
    println(rawDataRDD.partitions.length)

    //2、将源数据的每一行转成每一个Polygon
    val polygonRDD: RDD[PolygonInfo] = rawDataRDD.flatMap(PolygonInfo.parse(_))

    polygonRDD.cache() // 将解析完的RDD缓存起来

    import scala.collection.JavaConversions._
    val Array(startLevel, endLevel) = levels.split(",")
    val partitionsPerLevel = partitionsPerLevelStr.split(",").map(_.toInt)


    (startLevel.toInt to endLevel.toInt).zip(partitionsPerLevel).par.foreach { case (level, partitions) => //3、针对每一个区域(Polygon)进行切片，每一个Polygon会被切成若干个切片
      //3、针对每一个区域(Polygon)进行切片，每一个Polygon会被切成若干个切片
      val tilesPerPolygonRDD: RDD[(String, PolygonInfo)] = polygonRDD.flatMap(polygonInfo => {
        val tiles: java.util.List[String] = Utils.getTiles(polygonInfo.polygon, level)
        tiles.map((_, polygonInfo))
      })

      //4、按照切片的名称进行分组
      val polygonsPerTileRDD: RDD[(String, Iterable[PolygonInfo])] = tilesPerPolygonRDD.groupByKey(partitions) //HashPartitioner

      println(polygonsPerTileRDD.partitions.length)

      //5、对每一个切片的数据进行计算features，然后encode成byte类型的数组
      val encodedTileRDD: RDD[(String, Array[Byte])] =  polygonsPerTileRDD.flatMap { case (tile, polygonInfoIter) =>
        val vte = new VectorTileEncoder(4096, 16, false)
        val xy = tile.split("_")
        polygonInfoIter.foreach(polygonInfo =>{
          val tempPolygon = polygonInfo.polygon.copy() //故意设计的，一开始的时候我是加上了copy
          Utils.convert2Pixel(tempPolygon, xy(0).toInt, xy(1).toInt, level)
          val attributes = new util.HashMap[String, String]()
          attributes.put("id", polygonInfo.id)
          attributes.put("name", polygonInfo.layerName)
          attributes.put("height", polygonInfo.height)
          vte.addFeature("hz_building", attributes, tempPolygon) //比较耗时
        })
        val encodeValue = vte.encode()
        if (encodeValue.length > 0) Some((tile, encodeValue)) else None
      }

      //6、将编码后的每一个切片的数据呢保存HBase
      encodedTileRDD.foreachPartition { iterator =>
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", zk)
        val conn = ConnectionFactory.createConnection(conf)
        val table = conn.getTable(TableName.valueOf(tableName))
        val puts = new util.ArrayList[Put]();
        iterator.foreach { case (tile, encodedTile) =>
          val put = new Put(Bytes.toBytes(s"${tile.reverse}_${level}"))
          //最终的rowkey：反转切片的名称_level
          put.addColumn(Bytes.toBytes("f"), null, encodedTile)
          puts.add(put)
        }

        table.put(puts)
        table.close()
        conn.close()
      }

    }

    spark.stop()
    println(System.currentTimeMillis() - startTime)

  }

}
