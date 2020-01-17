package com.westar.dataset.sql.udaf
import com.westar.dataset.Utils._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
object OtherApiTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ActionApiTest")
      .master("local")
      .getOrCreate()

    val sessionDF = spark.read.load(s"${BASE_PATH}/trackerSession")

    val df = sessionDF.select("session_id", "session_server_time", "landing_url")
    //action api
    df.show()
    df.show(false)

    df.head()

    df.foreach(row => {
      println(row)
    })

    df.foreachPartition(iter => {
      iter.foreach(println(_))
    })

    df.take(1)

    df.count()

    df.collect()

    df.reduce((row1, row2) => Row(s"${row1.get(0)}#${row2.get(0)}"))


    //other api 和RDD中的api的原理以及含义是一样的
    sessionDF.coalesce(2)
    sessionDF.repartition(2)

    sessionDF.cache()
    sessionDF.persist(StorageLevel.MEMORY_AND_DISK)

    sessionDF.checkpoint()

    sessionDF.randomSplit(Array(0.2, 0.8))

    spark.stop()
  }

}
