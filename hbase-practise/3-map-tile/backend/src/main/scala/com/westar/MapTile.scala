package com.westar

import java.io.{BufferedOutputStream, File, FileOutputStream}
import java.util

import no.ecc.vectortile.VectorTileEncoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MapTile {
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()

    val spark = SparkSession.builder()
      .appName("MapTile")
      .master("local[*]")
      .getOrCreate()

    //1、读取数据源
    val rawDataRDD = spark.sparkContext.textFile("backend/src/main/resources/hz_building.csv")
    // rawDataRDD有几个分区
    // 如果是local方式的，那么这个分区数是1
    // 如果是local[n] (n > 1)：那么这个分区数是2
    print(rawDataRDD.partitions.length)
    //2、将源数据的每一行转成每一个Polygon

    val polygonRDD:RDD[PolygonInfo] = rawDataRDD.flatMap(PolygonInfo.parse(_))
    val partitionsPerLevel = Array(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 3, 4, 5, 6, 6, 6)
    (0 to 0).zip(partitionsPerLevel).par.foreach{ case (level, partitions) =>
      //3、针对每一个区域(Polygon)进行切片，每一个Polygon会被切成若干个切片
      val tilesPerPolygonRDD:RDD[(String,PolygonInfo)] = polygonRDD.flatMap(polygonInfo =>{
        val tiles: java.util.List[String] = Utils.getTiles(polygonInfo.polygon, level)
        import scala.collection.JavaConversions._
        tiles.map((_, polygonInfo))
      })

      //4、按照切片的名称进行分组
      val polygonsPerTileRDD: RDD[(String, Iterable[PolygonInfo])] = tilesPerPolygonRDD.groupByKey(partitions) //HashPartitioner
      println(polygonsPerTileRDD.partitions.length)

      //5、对每一个切片的数据进行计算features，然后encode成byte类型的数组
      val encodedTileRDD:RDD[(String,Array[Byte])] =polygonsPerTileRDD.flatMap{ case(tile, polygonInfoIter)=>
        val vte = new VectorTileEncoder(4096,16,false)
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
      //6、将编码后的每一个切片的数据呢保存到本地文件
      // 写文件也是一个很耗时的程序
      encodedTileRDD.foreach{case(tile, encodedTile)=>
          try{
            val path = "D:/pbf4/"
            val pbfFile = new File(path + File.separator + level)
            if (!pbfFile.exists()) pbfFile.mkdirs()
            //如果文件夹目录不存在，就创建
            val bos = new BufferedOutputStream(new FileOutputStream(path + level + File.separator + tile + ".pbf"))
            bos.write(encodedTile)
            System.out.println(Thread.currentThread().getName + "写入文件" + tile + ".pbf")
            bos.flush()
            bos.close()
          }catch {
            case e:Exception =>
              Console.err.println(s"error: ${e}")
          }
      }
    }

    spark.stop()
    println(System.currentTimeMillis() - startTime)
  }

}
