package com.westar.dataset.sql.example.complextype

import com.westar.dataset.Utils._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
 * get_json_object
 * from_json
 * to_json
 * selectExpr
 */
object JsonSQLFunctionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("JsonSQLFunctionTest")
      .master("local").getOrCreate()

    import spark.implicits._

    val eventsRDD =
      spark.sparkContext.textFile(s"${BASE_PATH}/example/device_info.txt").map(line => {
        val data = line.split("::")
        Row(data(0).toLong, data(1))
      })

    val schema = StructType(StructField("id", LongType)
      :: StructField("device", StringType) :: Nil)
    val eventsDF = spark.createDataFrame(eventsRDD, schema)

    eventsDF.printSchema()
    eventsDF.show(false)

    import org.apache.spark.sql.functions._

    //get_json_object
    eventsDF.select($"id", get_json_object($"device", "$.device_type").alias("device_type"),
      get_json_object($"device", "$.ip").alias("ip"),
      get_json_object($"device", "$.cca3").alias("cca3")).show()

    //from_json
    val fieldSeq = Seq(StructField("battery_level", LongType), StructField("c02_level", LongType),
      StructField("cca3",StringType), StructField("cn", StringType),
      StructField("device_id", LongType), StructField("device_type", StringType),
      StructField("signal", LongType), StructField("ip", StringType),
      StructField("temp", LongType), StructField("timestamp", TimestampType))
    val jsonSchema = StructType(fieldSeq)
    val devicesDF = eventsDF.select(from_json($"device", jsonSchema) as "devices")
    devicesDF.printSchema()
    devicesDF.show(false)

    val devicesAll = devicesDF.select($"devices.*").filter($"devices.temp" > 10
      and $"devices.signal" > 15)
    devicesAll.show()

    val devicesUSDF =
      devicesAll.select($"*").where($"cca3" === "USA").orderBy($"signal".desc, $"temp".desc)
    devicesUSDF.show()

    //to_json
    val stringJsonDF = eventsDF.select(to_json(struct($"*"))).toDF("devices")
    stringJsonDF.printSchema()
    stringJsonDF.show(false)

    stringJsonDF.write.mode(SaveMode.Overwrite).parquet(s"${BASE_PATH}/to_json")
    val parquetDF = spark.read.parquet(s"${BASE_PATH}/to_json")
    parquetDF.show(false)

    //selectExpr
    val stringsDF = eventsDF.selectExpr("CAST(id AS INT)", "CAST(device AS STRING)")
    stringsDF.show()

    val selectExprDF =
      devicesAll.selectExpr("c02_level",
        "round(c02_level/temp, 2) as ratio_c02_temperature").orderBy($"ratio_c02_temperature" desc)
    selectExprDF.show()

    spark.stop()
  }

}
case class DeviceData (id: Long, device: String)