package com.westar.dataset.creation.schema

import com.westar.dataset.Utils
import org.apache.spark.sql.{Row, SaveMode, SparkSession, types}
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructField, StructType}

object SchemaApiTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SchemaApiTest")
      .getOrCreate()

    val iotDeviceDf = spark.read.json(s"${Utils.BASE_PATH}/IoT_device_info.json")

    iotDeviceDf.toString()

    //1: schema的展示
    iotDeviceDf.schema
    iotDeviceDf.printSchema()

    //2: schema中可以有复杂数据类型
    val schema = StructType(
      StructField("name",StringType,false)::
      StructField("age",IntegerType,false)::
        StructField("map",MapType(StringType,StringType),true)::
        StructField("array",ArrayType(StringType),true)::
        StructField("struct",
          StructType(Seq(StructField("field1", StringType), StructField("field2", StringType))))
         :: Nil)

    val people = spark.sparkContext.parallelize(Seq("tom,30", "katy, 46")).map( _.split(",")).map(p =>
      Row(p(0), p(1).trim.toInt, Map(p(0) -> p(1)), Seq(p(0), p(1)), Row("value1", "value2"))
    )
    val dataFrame = spark.createDataFrame(people,schema)
    dataFrame.schema
    dataFrame.printSchema()

    dataFrame.select("map").collect().map(row => row.getAs[Map[String, String]]("map"))
    dataFrame.select("array").collect().map(row => row.getAs[Seq[String]]("array"))
    dataFrame.select("struct").collect().map(row => row.getAs[Row]("struct"))

    //schema 的用处
    val exampleSchema = new StructType().add("name", StringType).add("age", IntegerType)
    exampleSchema("name")
    exampleSchema.fields
    exampleSchema.fieldNames
    exampleSchema.fieldIndex("name")


    //1：查看一个parquet文件的schema
    val sessionDf = spark.read.parquet(s"${Utils.BASE_PATH}/trackerSession")
    sessionDf.schema
    sessionDf.printSchema()

    //2：比对两个parquet文件的schema是否相同
    val changedSchemaFieldNames = sessionDf.schema.fieldNames.map(fieldName =>
      if (fieldName == "pageview_count") {
        "pv_count"
      } else fieldName
    )
    sessionDf.toDF(changedSchemaFieldNames:_*).write.mode(SaveMode.Overwrite).parquet(s"${Utils.BASE_PATH}/trackerSession_changeSchema")
    val schemaChangeSessionDf = spark.read.parquet(s"${Utils.BASE_PATH}/trackerSession_changeSchema")
    schemaChangeSessionDf.schema
    schemaChangeSessionDf.printSchema()

    val oldSchema = sessionDf.schema
    val changeSchema = schemaChangeSessionDf.schema
    oldSchema == changeSchema //false

    //3：两个parquet文件的schema不一样，需要进行统一
    val allSessionError
    = spark.read.parquet(s"${Utils.BASE_PATH}/trackerSession", s"${Utils.BASE_PATH}/trackerSession_changeSchema")
    allSessionError.schema
    allSessionError.printSchema()

    val allSessionRight = sessionDf.toDF(changeSchema.fieldNames:_*).union(schemaChangeSessionDf)
    allSessionRight.printSchema()
    allSessionRight.show()

    spark.stop()

  }

}
