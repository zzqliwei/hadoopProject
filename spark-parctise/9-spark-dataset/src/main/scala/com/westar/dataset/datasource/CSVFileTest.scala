package com.westar.dataset.datasource

import com.westar.dataset.Utils
import org.apache.hadoop.hive.metastore.api.Schema
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DateType, DoubleType, FloatType, IntegerType, LongType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * //1: sep 和 delimiter的功能都是一样，都是表示csv的切割符，(默认是,)(读写参数)
 * //2: header(默认是false) 表示是否将csv文件中的第一行作为schema(读写参数)
 * //3: inferSchema 表示是否支持从数据中推导出schema(只读参数)
 * //4: charset和encoding(默认是UTF-8)，根据指定的编码器对csv文件进行解码(只读参数)
 * //5: quote(默认值是`"` ) 表示将不需要切割的字段值用quote标记起来(读写参数)
 * //6: escape(默认值是`\`) 如果在quote标记的字段值中还含有quote,则用escape来避免(读写参数)
 * //7: comment(默认是空字符串，表示关闭这个功能) 表示csv中的注释的标记符(读写参数)
 * //8: (读写参数)
 * //ignoreLeadingWhiteSpace(默认是false) 表示是否忽略字段值前面的空格
 * //ignoreTrailingWhiteSpace(默认是false) 表示是否忽略字段值后面的空格
 *
 * //9: multiLine(默认是false) 是否支持一条记录被拆分成了多行的csv的读取解析(只读参数)
 *
 * //10: mode (默认是PERMISSIVE) (只读参数)
 * //PERMISSIVE 表示碰到解析错误的时候，将字段都置为null
 * //DROPMALFORMED 表示忽略掉解析错误的记录
 * //FAILFAST 当有解析错误的时候，立马抛出异常
 *
 * //11: nullValue(默认是空字符串)， 表示需要将nullValue指定的字符串解析成null(读写参数)
 *
 * //12: nanValue(默认值为NaN) (只读参数)
 * //positiveInf
 * //negativeInf
 *
 * //13: codec和compression 压缩格式，支持的压缩格式有：
 * //none 和 uncompressed表示不压缩
 * //bzip2、deflate、gzip、lz4、snappy (只写参数)
 *
 *  //14 dateFormat (读写参数)
 *  //15: timestampFormat (读写参数)
 *
 *  //16: maxColumns(默认是20480) 规定一个csv的一条记录最大的列数 (只读参数)
 *
 */
object CSVFileTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    if(!conf.contains("spark.master")) {
      conf.setMaster("local[*]")
    }
    val spark = SparkSession
      .builder()
      .appName("CSVFileTest")
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val df = spark.read.json(s"${Utils.BASE_PATH}/people.json")
    //将json文件数据转化成csv文件数据
    df.write.mode(SaveMode.Overwrite).csv(s"${Utils.BASE_PATH}/csv")

    val csvDF = spark.read.csv(s"${Utils.BASE_PATH}/csv").toDF("age","name")
    csvDF.show()

    //从String类型中的Dataset来创建DataFrame
    val csvDS = spark.createDataset(Seq("23,jeffy", "34,katy"))
    val ds = spark.read.csv(csvDS).toDF("age","name")
    ds.show()

    //1: sep 和 delimiter的功能都是一样，都是表示csv的切割符，(默认是,)(读写参数)
    spark.read.csv(Seq("23,jeffy", "34,katy").toDS()).show()
    spark.read.option("sep"," ").csv(Seq("23 jeffy", "34 katy").toDS()).show()
    spark.read.option("delimiter"," ").csv(Seq("23 jeffy", "34 katy").toDS()).show()
    ds.write.mode(SaveMode.Overwrite).option("sep","|").csv(s"${Utils.BASE_PATH}/delimiter")

    //2: header(默认是false) 表示是否将csv文件中的第一行作为schema(读写参数)
    spark.read.csv(s"${Utils.BASE_PATH}/cars.csv").show()
    /*
    +----+-----+-----+--------------------+-----+
    | _c0|  _c1|  _c2|                 _c3|  _c4|
    +----+-----+-----+--------------------+-----+
    |year| make|model|             comment|blank|
    |2012|Tesla|    S|          No comment| null|
    |1997| Ford| E350|Go get one now th...| null|
    |2015|Chevy| Volt|                null| null|
    +----+-----+-----+--------------------+-----+
     */
    val headerDF = spark.read.option("header","true").csv(s"${Utils.BASE_PATH}/cars.csv")
    headerDF.printSchema()

    /**
    root
       |-- year: string (nullable = true)
       |-- make: string (nullable = true)
       |-- model: string (nullable = true)
       |-- comment: string (nullable = true)
       |-- blank: string (nullable = true)
     */

    headerDF.write.mode(SaveMode.Overwrite).option("header", true).csv(s"${Utils.BASE_PATH}/headerDF")
    headerDF.show()
    /*
    +----+-----+-----+--------------------+-----+
    |year| make|model|             comment|blank|
    +----+-----+-----+--------------------+-----+
    |2012|Tesla|    S|          No comment| null|
    |1997| Ford| E350|Go get one now th...| null|
    |2015|Chevy| Volt|                null| null|
    +----+-----+-----+--------------------+-----+
     */

    //3: inferSchema 表示是否支持从数据中推导出schema(只读参数)
    val inferSchemaDF = spark.read.option("header", true)
        .option("inferSchema",true)
        .csv(s"${Utils.BASE_PATH}/cars.csv")
    inferSchemaDF.printSchema()
    /*
    root
     |-- year: integer (nullable = true)
     |-- make: string (nullable = true)
     |-- model: string (nullable = true)
     |-- comment: string (nullable = true)
     |-- blank: string (nullable = true)
     */
    inferSchemaDF.show()
    /*
    +----+-----+-----+--------------------+-----+
    |year| make|model|             comment|blank|
    +----+-----+-----+--------------------+-----+
    |2012|Tesla|    S|          No comment| null|
    |1997| Ford| E350|Go get one now th...| null|
    |2015|Chevy| Volt|                null| null|
    +----+-----+-----+--------------------+-----+
     */

    //4: charset和encoding(默认是UTF-8)，根据指定的编码器对csv文件进行解码(只读参数)
    spark.read.option("header",true)
        .option("encoding","iso-8859-1")
        .option("sep","þ")
      .csv(s"${Utils.BASE_PATH}/cars_iso-8859-1.csv").show()
    /*
      +----+-----+-----+--------------------+-----+
      |year| make|model|             comment|blank|
      +----+-----+-----+--------------------+-----+
      |2012|Tesla|    S|          No comment| null|
      |1997| Ford| E350|Go get one now th...| null|
      |2015|Chevy| Volt|                null| null|
      +----+-----+-----+--------------------+-----+
       */

    //5: quote(默认值是`"` ) 表示将不需要切割的字段值用quote标记起来(读写参数)
    var optMap = Map("quote" -> "\'", "delimiter" -> " ")
    spark.read.options(optMap).csv(Seq("23 'jeffy tang'", "34 katy").toDS()).show()
    /*
    +---+----------+
    |_c0|       _c1|
    +---+----------+
    | 23|jeffy tang|
    | 34|      katy|
    +---+----------+
     */

    //6: escape(默认值是`\`) 如果在quote标记的字段值中还含有quote,则用escape来避免(读写参数)
    optMap = Map("quote" -> "\'", "delimiter" -> " ","escape" -> "\"")
    spark.read.options(optMap).csv(Seq("23 'jeffy \"tang'", "34 katy").toDS()).show()

    //7: comment(默认是空字符串，表示关闭这个功能) 表示csv中的注释的标记符(读写参数)
    optMap = Map("comment" -> "~", "header" -> "false")
    spark.read.options(optMap).csv(s"${Utils.BASE_PATH}/comments.csv").show()
    /*
        +---+---+---+---+----+-------------------+
        |_c0|_c1|_c2|_c3| _c4|                _c5|
        +---+---+---+---+----+-------------------+
        |  1|  2|  3|  4|5.01|2015-08-20 15:57:00|
        |  6|  7|  8|  9|   0|2015-08-21 16:58:01|
        |  1|  2|  3|  4|   5|2015-08-23 18:00:42|
        +---+---+---+---+----+-------------------+
         */
    optMap = Map("ignoreLeadingWhiteSpace" -> "true", "ignoreTrailingWhiteSpace" -> "true")
    spark.read.options(optMap).csv(Seq(" a,b  , c ").toDS()).show()
    val primitiveFieldAndType = Seq(
      """"
        |string","integer
        |
        |
        |","long
        |
        |","bigInteger",double,boolean,null""".stripMargin,
      """"this is a
        |simple
        |string.","
        |
        |10","
        |21474836470","92233720368547758070","
        |
        |1.7976931348623157E308",true,""".stripMargin)
    primitiveFieldAndType.toDF("value").coalesce(1)
        .write.mode(SaveMode.Overwrite).text(s"${Utils.BASE_PATH}/csv_multiLine_test")

    spark.read.option("header", true).option("multiLine", true).csv("csv_multiLine_test").show()

    //10: mode(默认是PERMISSIVE) (只读参数)
    //PERMISSIVE 表示碰到解析错误的时候，将字段都置为null
    //DROPMALFORMED 表示忽略掉解析错误的记录
    //FAILFAST 当有解析错误的时候，立马抛出异常
    val schema =  new StructType()
        .add("a", IntegerType)
        .add("b",TimestampType)
    val df1 = spark.read.option("mode","PERMISSIVE")
        .schema(schema).csv(Seq("0,2013-111-11 12:13:14", "1,1983-08-04").toDS())
    df1.show()

    //11: nullValue(默认是空字符串)， 表示需要将nullValue指定的字符串解析成null(读写参数)
    spark.read.option("nullValue", "--").csv(Seq("0,2013-11-11,--", "1,1983-08-04,3").toDS()).show()

    //12: nanValue(默认值为NaN) (只读参数)
    //positiveInf
    //negativeInf
    val numbers = spark.read.format("csv").schema(StructType(List(
      StructField("int", IntegerType, true),
      StructField("long", LongType, true),
      StructField("float", FloatType, true),
      StructField("double", DoubleType, true)
    ))).options(Map(
      "header" -> "true",
      "mode" -> "DROPMALFORMED",
      "nullValue" -> "--",
      "nanValue" -> "NAN",
      "negativeInf" -> "-INF",
      "positiveInf" -> "INF"
    )).load(s"${Utils.BASE_PATH}/numbers.csv")
    numbers.show()
    /*
        +----+--------+---------+---------------+
        | int|    long|    float|         double|
        +----+--------+---------+---------------+
        |   8| 1000000|    1.042|2.38485450374E7|
        |null|34232323|   98.343|184721.23987223|
        |  34|    null|   98.343|184721.23987223|
        |  34|43323123|     null|184721.23987223|
        |  34|43323123|223823.95|           null|
        |  34|43323123| 223823.0|            NaN|
        |  34|43323123| 223823.0|       Infinity|
        |  34|43323123| 223823.0|      -Infinity|
        +----+--------+---------+---------------+
         */

    //13: codec和compression 压缩格式，支持的压缩格式有：
    //none 和 uncompressed表示不压缩
    //bzip2、deflate、gzip、lz4、snappy (只写参数)
    inferSchemaDF.write.mode(SaveMode.Overwrite).option("compression", "gzip").csv(s"${Utils.BASE_PATH}/compression")

    //14 dateFormat (读写参数)
    val customSchema = new StructType(Array(StructField("date", DateType, true)))
    val date1 = spark.read.option("dateFormat","dd/MM/yyyy HH:mm").schema(customSchema)
        .csv(Seq("26/08/2015 18:00", "27/10/2014 18:30").toDS())
    date1.printSchema()
    /*
    root
      |-- date: date (nullable = true)
     */
    date1.write.mode(SaveMode.Overwrite).option("dateFormat", "yyyy-MM-dd").csv(s"${Utils.BASE_PATH}/dateFormat")
    spark.read.csv(s"${Utils.BASE_PATH}/dateFormat").show()

    //15: timestampFormat (读写参数)
    val timeSchema = new StructType(Array(StructField("date", TimestampType, true)))
    val time =
      spark.read.option("timestampFormat", "dd/MM/yyyy HH:mm").schema(timeSchema).csv(Seq("26/08/2015 18:00", "27/10/2014 18:30").toDS())
    time.printSchema()
    /*
    root
      |-- date: timestamp (nullable = true)
     */
    time.write.mode(SaveMode.Overwrite).option("timestampFormat", "yyyy-MM-dd HH:mm").csv(s"${Utils.BASE_PATH}/timestampFormat")
    spark.read.csv(s"${Utils.BASE_PATH}/timestampFormat").show()

    //16: maxColumns(默认是20480) 规定一个csv的一条记录最大的列数 (只读参数)
    spark.read.option("maxColumns", "3").csv(Seq("test,as,g", "h,bm,s").toDS()).show() //会报错
    spark.stop()
  }


}
