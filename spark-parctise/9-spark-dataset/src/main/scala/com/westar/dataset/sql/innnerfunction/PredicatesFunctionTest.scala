package com.westar.dataset.sql.innnerfunction

import org.apache.spark.sql.SparkSession

/**
 * 逻辑函数
 * and or not in
 */
object PredicatesFunctionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("appName")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val dataSeq = Seq("Table,Blue,124", "Table,Red,223", "Chair,Blue,101", "Chair,Red,210")
    val df = spark.read.csv(dataSeq.toDS).toDF("Item", "Color", "Quantity")
    df.createOrReplaceGlobalTempView("Inventory")

    //and
    spark.sql("select * from Inventory where Item = 'Table' and Color = 'Red'").show()
    //or
    spark.sql("select * from Inventory where Item = 'Table' or Color = 'Red'").show()
    //not
    spark.sql("select * from Inventory where not (Item = 'Table' or Color = 'Red')").show()
    //in
    spark.sql("select * from Inventory where Item in ('Table', 'Chair')").show()
    spark.stop()
  }
}
