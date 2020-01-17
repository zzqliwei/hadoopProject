package com.westar.dataset.datasource

import com.westar.dataset.Utils
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType, TimestampType}
import org.joda.time.DateTimeFieldType

/**
 * //primitivesAsString(默认为false) 表示将基本类型转化为string类型，这里的基本类型包括：boolean、int、long、float、double
 * //prefersDecimal(默认是false)表示在primitivesAsString为false的时候，将float，double转成DecimalType
 *
 * //allowComments(默认是false)，表示是否支持json中含有java/c格式的注释
 * //allowUnquotedFieldNames(默认是false)，表示是否支持json中含有没有引号的域名
 * //allowSingleQuotes(默认是true)，表示是否支持json中含有单引号的域名或者值
 * //allowNumericLeadingZeros(默认是false)，表示是否支持json中含有以0开头的数值
 * //allowNonNumericNumbers(默认是false)，表示是否支持json中含有NaN(not a number)
 * //allowBackslashEscapingAnyCharacter(默认是false)，表示是否支持json中含有反斜杠，且将反斜杠忽略掉
 *
 *
 * //mode(默认是PERMISSIVE)，表是碰到格式解析错误的json的处理行为是：
 * //PERMISSIVE 表示比较宽容的。如果某条格式错误，则新增一个字段，字段名为columnNameOfCorruptRecord的值，字段的值是错误格式的json字符串，其他的是null
 * //DROPMALFORMED 表示丢掉错误格式的那条记录
 *  //FAILFAST 碰到解析错误的记录直接报错
 *
 *  //dateFormat(默认值为yyyy-MM-dd) 表示json中时间的字符串格式(对应着DataType)
 *  //timestampFormat(默认值为yyyy-MM-dd'T'HH:mm:ss.SSSZZ) 表示json中时间的字符串格式(对应着TimestampType)
 *
 * //compression 压缩格式，支持的压缩格式有：
 * //none 和 uncompressed表示不压缩
 * //bzip2、deflate、gzip、lz4、snappy
 *
 *
 */
object JsonFileTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("JsonFileTest")
      .getOrCreate()

    import spark.implicits._

    //将parquet文件数据转化成json文件数据
    val sessionDf = spark.read.parquet(s"${Utils.BASE_PATH}/trackerSession")
    sessionDf.show()

    sessionDf.write.json(s"${Utils.BASE_PATH}/json")

    //读取json文件数据
    val jsonDF = spark.read.json(s"${Utils.BASE_PATH}/json")
    jsonDF.show()

    //可以从JSON Dataset(类型为String)中创建一个DF
    val jsonDataset = spark.createDataset(
      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val otherJsonDF = spark.read.json(jsonDataset)
    otherJsonDF.show()

    //primitivesAsString(默认为false) 表示将基本类型转化为string类型，这里的基本类型包括：boolean、int、long、float、double
    //prefersDecimal(默认是false)表示在primitivesAsString为false的时候，将float，double转成DecimalType
    val jsonDataset_1 = spark.createDataset(
      """{"name":"Yin","address":{"is_old":true,"area":23000.34}}""" :: Nil)
    var otherJsonDF_1 = spark.read.json(jsonDataset_1)
    otherJsonDF_1.printSchema()
    /*
    root
     |-- address: struct (nullable = true)
     |    |-- area: double (nullable = true)
     |    |-- is_old: boolean (nullable = true)
     |-- name: string (nullable = true)
     */

    var optsMap = Map("primitivesAsString" -> "true", "prefersDecimal" -> "true")
    otherJsonDF_1 = spark.read.options(optsMap).json(jsonDataset_1)
    otherJsonDF_1.printSchema()
    /*
    root
     |-- address: struct (nullable = true)
     |    |-- area: string (nullable = true)
     |    |-- is_old: string (nullable = true)
     |-- name: string (nullable = true)
     */
    optsMap = Map("primitivesAsString" -> "false", "prefersDecimal" -> "true")
    otherJsonDF_1 = spark.read.options(optsMap).json(jsonDataset_1)
    otherJsonDF_1.printSchema()
    /*
   root
    |-- address: struct (nullable = true)
    |    |-- area: decimal(7,2) (nullable = true)
    |    |-- is_old: boolean (nullable = true)
    |-- name: string (nullable = true)
    */
    //allowComments(默认是false)，表示是否支持json中含有java/c格式的注释
    spark.read.option("allowComments", "true").json(Seq("""{"name":/* hello */"Yin","address":{"is_old":true,"area":23000.34}}""").toDS()).show()

    //allowUnquotedFieldNames(默认是false)，表示是否支持json中含有没有引号的域名
    spark.read.option("allowSingleQuotes", "true").json(Seq("""{'name':'Yin',"address":{"is_old":true,"area":23000.34}}""").toDS()).show()
	
    //allowNumericLeadingZeros(默认是false)，表示是否支持json中含有以0开头的数值
    spark.read.option("allowNumericLeadingZeros", "true").json(Seq("""{'name':'Yin',"address":{"is_old":true,"area":0023000.34}}""").toDS()).show()

    //allowNonNumericNumbers(默认是false)，表示是否支持json中含有NaN(not a number)
    spark.read.option("allowNonNumericNumbers", "true").json(Seq("""{'name':'Yin',"address":{"is_old":true,"area":NaN}}""").toDS()).show()

    //allowBackslashEscapingAnyCharacter(默认是false)，表示是否支持json中含有反斜杠，且将反斜杠忽略掉
    spark.read.option("allowBackslashEscapingAnyCharacter", "true").json(Seq("""{'name':'Yin',"address":{"is_old":true,"area":"\$23000"}}""").toDS()).show()

    //mode(默认是PERMISSIVE)，表是碰到格式解析错误的json的处理行为是：
    //PERMISSIVE 表示比较宽容的。如果某条格式错误，则新增一个字段，字段名为columnNameOfCorruptRecord的值，字段的值是错误格式的json字符串，其他的是null
    spark.read.option("mode", "PERMISSIVE").json(Seq("""{'name':'Yin',"address":{"is_old":true,"area":3000}}""",
      """{'name':'Yin',"address":{"is_old":true,"area":\3000}}""").toDS()).show()
    /*
    +--------------------+-----------+----+
    |     _corrupt_record|    address|name|
    +--------------------+-----------+----+
    |                null|[3000,true]| Yin|
    |{'name':'Yin',"ad...|       null|null|
    +--------------------+-----------+----+
     */
    spark.read.option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "customer_column").json(
      Seq("""{'name':'Yin',"address":{"is_old":true,"area":3000}}""",
        """{'name':'Yin',"address":{"is_old":true,"area":\3000}}""").toDS()).show()
    /*
    +-----------+--------------------+----+
    |    address|     customer_column|name|
    +-----------+--------------------+----+
    |[3000,true]|                null| Yin|
    |       null|{'name':'Yin',"ad...|null|
    +-----------+--------------------+----+
     */
    //DROPMALFORMED 表示丢掉错误格式的那条记录
    spark.read.option("mode", "DROPMALFORMED").json(Seq("""{'name':'Yin',"address":{"is_old":true,"area":3000}}""",
      """{'name':'Yin',"address":{"is_old":true,"area":\3000}}""").toDS()).show()
    /*
    +-----------+----+
    |    address|name|
    +-----------+----+
    |[3000,true]| Yin|
    +-----------+----+
     */
    spark.read.option("mode", "FAILFAST").json(Seq("""{'name':'Yin',"address":{"is_old":true,"area":3000}}""",
      """{'name':'Yin',"address":{"is_old":true,"area":\3000}}""").toDS()).show()


    //dateFormat(默认值为yyyy-MM-dd) 表示json中时间的字符串格式(对应着DataType)
    val customSchema = new StructType(Array(StructField("name", StringType, true),StructField("date", DateType, true) ))
    val dataFormatDF = spark.read.schema(customSchema).option("dateFormat", "dd/MM/yyyy HH:mm").json(Seq(
      """{'name':'Yin',"date":"26/08/2015 18:00"}""").toDS())
    dataFormatDF.write.mode(SaveMode.Overwrite).option("dateFormat", "yyyy/MM/dd").json(s"${Utils.BASE_PATH}/testjson")
    spark.read.json(s"${Utils.BASE_PATH}/testjson").show()

    //timestampFormat(默认值为yyyy-MM-dd'T'HH:mm:ss.SSSZZ) 表示json中时间的字符串格式(对应着TimestampType)
    val customSchema_1 = new StructType(Array(StructField("name", StringType, true),
      StructField("date", TimestampType, true)))
    val timestampFormatDf =
      spark.read.schema(customSchema_1).option("timestampFormat", "dd/MM/yyyy HH:mm").json(Seq(
        """{'name':'Yin',"date":"26/08/2015 18:00"}""").toDS())

    val optMap = Map("timestampFormat" -> "yyyy/MM/dd HH:mm",DateTimeUtils.TIMEZONE_OPTION -> "GMT")
    timestampFormatDf.write.mode(SaveMode.Overwrite).options(optMap).json(s"${Utils.BASE_PATH}/test.json")
    spark.read.json(s"${Utils.BASE_PATH}/test.json").show()

    //compression 压缩格式，支持的压缩格式有：
    //none 和 uncompressed表示不压缩
    //bzip2、deflate、gzip、lz4、snappy
    val primitiveFieldAndType: Dataset[String] = spark.createDataset(spark.sparkContext.parallelize(
      """{"string":"this is a simple string.",
          "integer":10,
          "long":21474836470,
          "bigInteger":92233720368547758070,
          "double":1.7976931348623157E308,
          "boolean":true,
          "null":null
      }""" ::
        """{"string":"this is a simple string.",
          |          "integer":10,
          |          "long":21474836470,
          |          "bigInteger":92233720368547758070,
          |          "double":1.7976931348623157E308,
          |          "boolean":true,
          |          "null":null
          |      }""" :: Nil))(Encoders.STRING)
    primitiveFieldAndType.toDF("value").write.mode(SaveMode.Overwrite).option("compression", "GzIp").text(s"${Utils.BASE_PATH}/primitiveFieldAndType")

    val multiLineDF = spark.read.option("multiLine", false).json(s"${Utils.BASE_PATH}/primitiveFieldAndType")
    multiLineDF.show()

    spark.stop()

  }

}
