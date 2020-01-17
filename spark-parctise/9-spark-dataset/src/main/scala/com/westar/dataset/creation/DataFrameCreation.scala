package com.westar.dataset.creation

import com.westar.dataset.{Dog, Utils}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object DataFrameCreation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DataFrameCreation")
      .master("local")
      .getOrCreate()

    //1: 从RDD[A <: Product]中创建, case class 和 tuple都是Product的子类
    val rdd = spark.sparkContext.textFile("").map(line => {
      val splitData = line.split(" ")
      Dog(splitData(0), splitData(1))
    })

    val tupleRDD = spark.sparkContext.parallelize(Seq(("jitty", 2), ("mytty", 4)))

    spark.createDataFrame(rdd)
    spark.createDataFrame(tupleRDD)

    val dogRDD = spark.sparkContext.parallelize(Seq(Dog("jitty", "red"), Dog("mytty", "yellow")))
    val dogDf = spark.createDataFrame(dogRDD)
    dogDf.show()

    //2: 从Seq[A <: Product]中创建
    val dogSeq = Seq(Dog("jitty", "red"), Dog("mytty", "yellow"))
    spark.createDataFrame(dogSeq).show()

    //3:用RDD[_] + class创建，这个class是java的bean
    val catRDD = spark.sparkContext.parallelize(Seq(new Cat("jitty", 2), new Cat("mytty", 4)))
    val catDf = spark.createDataFrame(catRDD,classOf[Cat])
    catDf.show()

    catDf.createOrReplaceTempView("cat")
    spark.sql("select * from cat").show()//需要注意的是查询出来的cat的属性的顺序是不固定的

    //4: 用RDD[Row] + schema创建
    val rowSeq = Seq("tom,30", "katy, 46").map(_.split(",")).map(p => Row(p(0), p(1).trim.toInt))
    val rowRDD = spark.sparkContext.parallelize(rowSeq)

    val schema = StructType(
      Array(StructField("name", StringType, false),StructField("age", IntegerType, true))
    )
    val dataFrame = spark.createDataFrame(rowRDD,schema)
    dataFrame.printSchema()
    dataFrame.show()

    //5: 从外部数据源中创建
    val df = spark.read.json(s"${Utils.BASE_PATH}/IoT_device_info.json")
    df.show()

    spark.stop()


  }

}
