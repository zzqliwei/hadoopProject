package com.westar.dataset.sql.example.complextype

import org.apache.spark.sql.SparkSession
import com.westar.dataset.Utils._
import org.apache.spark.sql.types.{DoubleType, LongType, MapType, StringType, StructType}

/**
 * explode
 * getItem
 */
object JsonComplexTypeTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("JsonComplexTypeTest")
      .master("local")
      .getOrCreate()

    val dataRDD = spark.sparkContext.textFile(s"${BASE_PATH}/example/complex_type.json")

    val schema = new StructType()
      .add("dc_id", StringType)
      .add("source",
        MapType(
          StringType,
          new StructType()
            .add("description", StringType)
            .add("ip", StringType)
            .add("id", LongType)
            .add("temp", LongType)
            .add("c02_level", LongType)
            .add("geo",
              new StructType()
                .add("lat", DoubleType)
                .add("long", DoubleType)
              )
          )
    )

    import spark.implicits._
    val ds = spark.createDataset(dataRDD)

    val df = spark.read.schema(schema).json(ds)

    df.printSchema()
    df.show(false)

    import org.apache.spark.sql.functions._
    import spark.implicits._

    // explode 展开
    val explodedDF = df.select($"dc_id", explode($"source"))
    explodedDF.printSchema()
    explodedDF.show()

    //getItem
    val devicesDF = explodedDF.select( $"dc_id" as "dcId",
      $"key" as "deviceType",
      'value.getItem("ip") as 'ip,
      'value.getItem("id") as 'deviceId,
      'value.getItem("c02_level") as 'c02_level,
      'value.getItem("temp") as 'temp,
      'value.getItem("geo").getItem("lat") as 'lat,
      'value.getItem("geo").getItem("long") as 'lon)

    devicesDF.printSchema()
    devicesDF.show()

    spark.stop()
  }

}
