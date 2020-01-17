package com.westar.dataset.sql.udaf

import org.apache.spark.sql.SparkSession

object TypedKeyValueApiTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("TypedKeyValueApiTest")
      .getOrCreate()

    import spark.implicits._

    val ds1 = Seq(("a", 1), ("b", 1)).toDS()
    ds1.show()

    val grouped = ds1.groupByKey(_._2)
    grouped.keys.show()

    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()
    val keyValueGroupedDataset = ds.groupByKey(_._1)

    val agged = keyValueGroupedDataset.mapGroups{ case(key, iter) =>(key,iter.map(_._2).sum)}
    agged.show()

    val aggedFlatMapGroup = keyValueGroupedDataset.flatMapGroups{case(key, iter) =>
        Iterator(key,iter.map(_._2).sum.toString)
    }
    aggedFlatMapGroup.show()

    val keyValueMapValue = keyValueGroupedDataset.mapValues(_._2)
    val aggedMapValue = keyValueMapValue.mapGroups { case (key, iter) => (key, iter.sum) }
    aggedMapValue.show()

    val strDS = Seq("abc", "xyz", "hello").toDS()
    val aggedReduce = strDS.groupByKey(_.length).reduceGroups(_ + _)
    aggedReduce.show()


    spark.stop()
  }

}
