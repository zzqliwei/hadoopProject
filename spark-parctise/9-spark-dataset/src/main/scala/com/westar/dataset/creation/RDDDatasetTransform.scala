package com.westar.dataset.creation

import com.westar.dataset.Dog
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object RDDDatasetTransform {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("RDDDatasetTransform")
      .getOrCreate()

    val dogs = Seq(Dog("jitty", "red"), Dog("mytty", "yellow"))

    import spark.implicits._

    //1: RDD转DataFrame
    val dogRDD = spark.sparkContext.parallelize(dogs)
    val dogDF = dogRDD.toDF()
    dogDF.show()

    val renameSchemaDF = dogRDD.toDF("first_name", "lovest_color")
    renameSchemaDF.show()

    //2: DataFrame转RDD, schema信息丢掉了
    val dogRowRDD: RDD[Row] = renameSchemaDF.rdd
    dogRowRDD.collect()
    renameSchemaDF.rdd.collect()

    //3: RDD转Dataset
    val dogDS =dogRDD.toDS()
    dogDS.show()

    //4: Dataset转RDD
    val dogRDDFromDs: RDD[Dog] = dogDS.rdd
    dogRDDFromDs.collect()

    //5: DataFrame转Dataset
    val dogDsFromDf = renameSchemaDF.as[Dog]
    dogDsFromDf.show()

    //6: Dataset转DataFrame
    val dogDfFromDs= dogDsFromDf.toDF()
    dogDfFromDs.show()

    spark.stop()
  }

}
