package com.westar.dataset.sql.udaf

import com.westar.dataset.Person
import com.westar.dataset.Utils._
import org.apache.spark.sql.SparkSession

object TypedUDAFTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SqlApiTest")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val ds = spark.read.json(s"${BASE_PATH}/people.json").as[Person]
    ds.show()

    // Convert the function to a `TypedColumn` and give it a name
    val averageAge = TypedMyAverage.toColumn.name("average_age")
    val result = ds.select(averageAge)
    result.show()

    ds.groupByKey(_.name).agg(averageAge).show()

    spark.stop()
  }

}
