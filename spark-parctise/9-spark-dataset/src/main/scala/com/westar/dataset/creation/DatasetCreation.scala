package com.westar.dataset.creation

import java.beans.Encoder

import com.westar.dataset.Dog
import org.apache.spark.sql.{Encoders, SparkSession}

object DatasetCreation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkSessionTest")
      .getOrCreate()

    import spark.implicits._

    //1: range
    val ds1 = spark.range(0, 10, 2, 2)
    ds1.show()

    val dogs = Seq(Dog("jitty", "red"), Dog("mytty", "yellow"))
    val cats = Seq(new Cat("jitty", 2), new Cat("mytty", 4))

    //2: 从Seq[T]中创建
    val data = dogs
    val ds = spark.createDataset(data)
    ds.show()

    //3: 从RDD[T]中创建
    val dogRDD =spark.sparkContext.parallelize(dogs)
    val dogDS = spark.createDataset(dogRDD)
    dogDS.show()

    val catRDD = spark.sparkContext.parallelize(cats)
    //val catDSWithoutEncoder = spark.createDataset(catRDD)
    val catDS = spark.createDataset(catRDD)(Encoders.bean(classOf[Cat]))
    catDS.show()

    //Encoders 负责JVM对象类型与spark SQL内部数据类型之间的转换
    val intDs = Seq(1, 2, 3).toDS() // implicitly provided (spark.implicits.newIntEncoder)
    val seqIntDs = Seq(Seq(1), Seq(2), Seq(3)).toDS() // implicitly provided (spark.implicits.newIntSeqEncoder)
    val arrayIntDs = Seq(Array(1), Array(2), Array(3)).toDS() // implicitly provided (spark.implicits.newIntArrayEncoder)

    //支持的Encoders有如下：
    Encoders.product //tuples and case classes
    Encoders.scalaBoolean
    Encoders.scalaByte
    Encoders.scalaDouble
    Encoders.scalaFloat
    Encoders.scalaInt
    Encoders.scalaLong
    Encoders.scalaShort

    Encoders.bean(classOf[Cat])

    spark.stop()
  }

}
