package com.westar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 采用yarn 模式运行程序
 * export HADOOP_CONF_DIR=/home/hadoop/bigdata/hadoop-2.7.5/etc/hadoop
 * spark-submit --class com.westar.PlayerStatsPreprocess \
 * --master yarn \
 * --executor-memory 512m \
 * * --num-executors 2 \
 * * --executor-cores 1 \
 * /home/hadoop/nba/spark-nba-player-1.0-SNAPSHOT.jar \
 * hdfs://master:9999/user/hadoop/nba/basketball hdfs://master:9999/user/hadoop/nba/tmp
 *
 *
 * 文件读取的通配符需要注意
 */
object PlayerStatsPreprocess {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val configuration = new Configuration()
    if(!conf.contains("spark.master")){
      conf.setMaster("local[*]")
    }
    val spark = SparkSession.builder()
      .appName("PlayerStatsPreprocess")
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext

    val(rawDataPath,tmpPath) = if(args.isEmpty){
      ( "hive-practise/5-spark-nba-player/data/nba/basketball", "hive-practise/5-spark-nba-player/data/nba/tmp")
    }else{
      configuration.set("fs.defaultFS","hdfs://master:9999")
      (args(0),args(1))
    }
    //hdfs的操作api其实可以操作本地文件目录
    // Exception in thread "main" java.lang.IllegalArgumentException:
    // Wrong FS: hdfs://master:9999/user/hadoop/nba/tmp, expected: file:///
    // 设置参数 configuration.set("fs.defaultFS", "hdfs://master:9999")

    /*val fs = FileSystem.get(configuration)
    fs.delete(new Path(tmpPath),true)

    for(year <- 1980 to 2016){
      val yearRawData = sc.textFile(s"${rawDataPath}/leagues_NBA_${year}*")
      yearRawData.filter(line => !line.trim.isEmpty && !line.startsWith("Rk,")).map(line =>{
        val temp = line.replaceAll("\\*","").replaceAll(",,",",0,").replaceAll(",,",",0,")
        s"${year},${temp}"
      }).saveAsTextFile(s"${tmpPath}/${year}")
    }*/

    import spark.implicits._
    (1980 to 2016).map(year =>{
      sc.textFile(s"${rawDataPath}/leagues_NBA_${year}*")
        .filter(line => !line.trim.isEmpty && !line.startsWith("Rk,"))
        .map(line =>{
          val temp = line.replaceAll("\\*","").replaceAll(",,",",0,").replaceAll(",,", ",0,")
          (year,s"${year},${temp}")
        })
    }).reduce(_.union(_)).toDF("year","line")
        .write
        .mode(SaveMode.Overwrite)
//        .option("header",true)
        .partitionBy("year")
        .csv(tmpPath)

    spark.stop()

  }

}
