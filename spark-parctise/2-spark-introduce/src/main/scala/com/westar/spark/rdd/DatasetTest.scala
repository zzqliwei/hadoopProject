package com.westar.spark.rdd

import org.apache.spark.SparkConf
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object DatasetTest {
  private val logger = LoggerFactory.getLogger("DatasetTest")

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    if(!conf.contains("spark.master")){
      conf.setMaster("local[*]")
    }

    val spark = SparkSession.builder()
      .appName("DatasetTest")
      .config(conf)
      .getOrCreate()

    val df = spark.read.json("hdfs://hadoop0:9000/users/hadoop/person.json")

    df.schema
    df.rdd

    import spark.implicits._

    //DF sql的方式
    df.createOrReplaceTempView("person")
    val sqlDF = spark.sql("SELECT age, name FROM person where age > 21 ")
    sqlDF.show()

    //DF api的方式
    val filterAgeDf = df.filter($"age">21)
    filterAgeDf.show()

    // 以下是说明关系型数据处理与程序算法处理结合的代码
    // 用sql的方式准备数据
    val source1 = spark.read.parquet("data/mllib/als/sample_movielens_ratings1.parquet").toDF()

    source1.createOrReplaceTempView("source1")

    val source2 = spark.read.json("data/mllib/als/sample_movielens_ratings2.json").toDF()
    source2.createOrReplaceTempView("source2")

    //这个sql还可以更复杂，一切为了业务
    val training = spark.sql("select s1.user_id, s2.item_id, s1.rating " +
      "from source1 as s1 join source2 as s2 on s1.user_id = s2.user_id")

    // 将上面准备的数据运用机器学习
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(training)

    val personDF = spark.read.json("hdfs://hadoop0:9000/users/hadoop-twq/person.json")
    //Dataset的强类型
    val personDS = personDF.as[Person]

    val primitiveDS = Seq(1, 2, 3).toDS()
    //Dataset可以支持强大的lamda表达式
    val filterDS = personDS.filter(person =>{
      if(person.age.isDefined && person.age.get > 21){
        true
      }else{
        logger.info(s"======= my test filter ${person.age}")
        false
      }
    })
    filterDS.collect()

    // DataFrame是类型为Row的Dataset，即Dataset[Row]
    val personDf = personDS.toDF()

    //可以将Dataset理解成带有schema的RDD
    personDS.schema
    personDS.rdd

  }

  case class Person(name: String, age: Option[Long])

}
