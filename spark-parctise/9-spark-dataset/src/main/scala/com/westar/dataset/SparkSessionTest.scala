package com.westar.dataset

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object SparkSessionTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SparkSessionTest")
    conf.set("spark.sql.shuffle.partitions","6");

    conf.setMaster("local[*]")

    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Seq(1, 2, 3))

    val sqlContext = new SQLContext(sc)
    sqlContext.sql("select * from test")

    val hiveContext = new HiveContext(sc)
    hiveContext.sql("select * from db.test")

    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
    val spark = SparkSession.builder()
      .appName("SparkSessionTest")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    //1: 设置spark运行时的配置
    //set new runtime options

    import spark.implicits._
    spark.conf.set("spark.sql.shuffle.partitions","6")
    spark.conf.set("spark.executor.memory","2g")

    //get all settings
    val configMap:Map[String, String]  = spark.conf.getAll

    //2: 创建Dataset
    // 用spark.range创建一个Dataset

    val numDS = spark.range(1,10,2,2)
    numDS.orderBy(desc("id")).show()

    // 用spark.createDataFrame从List中创建一个DataFrame
    val langPercentDF = spark.createDataFrame(List(("Scala", 35), ("Python", 30), ("R", 15), ("Java", 20)))
    // 重命名DF中的列名
    val lpDF = langPercentDF.withColumnRenamed("_1","language").withColumnRenamed("_2", "percent")
    // 对df按照列percent进行逆序排序
    lpDF.orderBy(desc("percent")).show()

    // 从json文件中读取数据来创建DataFrame
    val personDF = spark.read.json(s"${Utils.BASE_PATH}/people.json")
    personDF.filter(personDF.col("age")>20).show()

    //3: 用sql api来执行sql的查询
    personDF.createOrReplaceTempView("people")
    personDF.cache()

    val resultsDF = spark.sql("select * from people")
    resultsDF.show()


    //4: 访问catalog元数据
    spark.catalog.listDatabases().show()
    spark.catalog.listTables().show()

    //5: 封装SparkContext
    val sparkContext = spark.sparkContext
    sparkContext.parallelize(Seq("1", "2"))

    spark.stop()



    }

}
