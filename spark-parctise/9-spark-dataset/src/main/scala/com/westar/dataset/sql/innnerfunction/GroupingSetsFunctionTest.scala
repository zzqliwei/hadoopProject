package com.westar.dataset.sql.innnerfunction

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 分组函数
 */
object GroupingSetsFunctionTest {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    if(!conf.contains("spark.master")){
      conf.setMaster("local[*]")
    }

    val spark = SparkSession.builder()
      .appName("GroupingSetsFunctionTest")
      .config(conf)
      .getOrCreate()

    //参考： https://msdn.microsoft.com/zh-cn/library/ms175939(SQL.90).aspx
    //  https://msdn.microsoft.com/zh-cn/library/ms189305(v=sql.90).aspx
    //  https://msdn.microsoft.com/zh-cn/library/bb510624(v=sql.105).aspx

    import spark.implicits._

    val dataSeq = Seq("Table,Blue,124", "Table,Red,223", "Chair,Blue,101", "Chair,Red,210")
    val df = spark.read.csv(dataSeq.toDS()).toDF("Item", "Color", "Quantity")
    df.createOrReplaceTempView("Inventory")

    //cube
    spark.sql(
      """
        |select Item,Color,sum(Quantity) as QtySum
        |from Inventory
        |group by Item,Color with cube
        |""".stripMargin).show()
    /*
        +-----+-----+------+
        | Item|Color|QtySum|
        +-----+-----+------+
        |Table| null| 347.0|
        |Table| Blue| 124.0|
        | null| null| 658.0|
        | null| Blue| 225.0|
        |Chair|  Red| 210.0|
        |Chair| null| 311.0|
        |Chair| Blue| 101.0|
        |Table|  Red| 223.0|
        | null|  Red| 433.0|
        +-----+-----+------+
         */

    //GROUPING + cube
    spark.sql(
      """
        |SELECT case when (GROUPING(Item) = 1)  then 'all'
        |             else nvl(Item,'UNKONWN')
        |       end as Item,
        |       case when (GROUPING(Color) = 1) then 'all'
        |             else nvl(Color,'UNKOOWN')
        |       end as Color,
        |       sum(Quantity) as QtySum
        |from Inventory
        |group by Item,Color with cube
        |""".stripMargin).show()

    /**
     * +-----+-----+------+
     * | Item|Color|QtySum|
     * +-----+-----+------+
     * |Table|  all| 347.0|
     * |Table| Blue| 124.0|
     * |  all|  all| 658.0|
     * |  all| Blue| 225.0|
     * |Chair|  Red| 210.0|
     * |Chair|  all| 311.0|
     * |Chair| Blue| 101.0|
     * |Table|  Red| 223.0|
     * |  all|  Red| 433.0|
     * +-----+-----+------+
     */


    //GROUPING + ROLLUP
    spark.sql(
      """
        |SELECT case when (GROUPING(Item) = 1)  then 'all'
        |             else nvl(Item,'UNKONWN')
        |       end as Item,
        |       case when (GROUPING(Color) = 1) then 'all'
        |             else nvl(Color,'UNKOOWN')
        |       end as Color,
        |       sum(Quantity) as QtySum
        |from Inventory
        |group by Item,Color with ROLLUP
        |""".stripMargin).show()

    /**
     * +-----+-----+------+
     * |Table|  all| 347.0|
     * |Table| Blue| 124.0|
     * |  all|  all| 658.0|
     * |Chair|  Red| 210.0|
     * |Chair|  all| 311.0|
     * |Chair| Blue| 101.0|
     * |Table|  Red| 223.0|
     * +-----+-----+------+
     */
    //GROUPING + ROLLUP + GROUPING_ID

    spark.sql(
      """
        |select case when (grouping(Item)=1) then 'all'
        |       else nvl(Item,'unknow')
        |       end as item,
        |       case when (grouping(Color)=1)then 'all'
        |       else nvl(Color,'unknow')
        |       end as color,
        |       GROUPING_ID(Item,Color) as GroupingId,
        |       sum(Quantity) as QtySum
        |from Inventory
        |group by Item,Color with ROLLUP
        |
        |""".stripMargin).show()

    /**
     * | item|color|GroupingId|QtySum|
     * +-----+-----+----------+------+
     * |Table|  all|         1| 347.0|
     * |Table| Blue|         0| 124.0|
     * |  all|  all|         3| 658.0|
     * |Chair|  Red|         0| 210.0|
     * |Chair|  all|         1| 311.0|
     * |Chair| Blue|         0| 101.0|
     * |Table|  Red|         0| 223.0|
     * +-----+-----+----------+------+
     */
    spark.stop()

  }

}
