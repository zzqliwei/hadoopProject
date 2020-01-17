package com.westar.dataset.sql.udaf

import org.apache.spark.sql.SparkSession
import com.westar.dataset.Utils._

object UntypedUDAFTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SqlApiTest")
      .master("local[*]")
      .getOrCreate()

    // Register the function to access it
    spark.udf.register("myAverage", UntypedMyAverage)

    val df = spark.read.json(s"${BASE_PATH}/people.json")
    df.createOrReplaceTempView("people")
    df.show()


    val result = spark.sql("SELECT myAverage(age) as average_age FROM people")
    result.show()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val myAverage = UntypedMyAverage
    df.groupBy("name").agg(myAverage($"age").as("average_age")).show()

    df.groupBy("name").agg(expr("myAverage(age) as average_age")).show()

    spark.stop()
  }

}
