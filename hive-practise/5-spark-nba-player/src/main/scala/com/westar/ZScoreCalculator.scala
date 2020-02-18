package com.westar

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

/**
 * 计算球员的得分信息
 * export HADOOP_CONF_DIR=/home/hadoop/bigdata/hadoop-2.7.5/etc/hadoop
 * spark-submit --class com.westar.ZScoreCalculator \
 * --master yarn \
 * --executor-memory 512m \
 * --num-executors 4 \
 * --executor-cores 2 \
 * /home/hadoop/nba/spark-nba-player-1.0-SNAPSHOT.jar hdfs://master:9999/user/hadoop/nba/tmp nba
 */
object ZScoreCalculator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    if(!conf.contains("spark.master")){
      conf.setMaster("local[*]")
    }

    val spark = SparkSession.builder()
      .appName("ZScoreCalculator")
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext

    val (yearRawDataPath, db) = if (args.isEmpty) ("hive-practise/5-spark-nba-player/data/nba/tmp", "default") else (args(0), args(1))

    //1、计算每一年每一个指标的平均数和标准方差
    //1.1 先要解析预处理数据之后的csv文件，生成PlayStats的Dataset
    import spark.implicits._
    val yearRawStats = sc.textFile(s"${yearRawDataPath}/*/*")
    val parsedPlayerStats:Dataset[PlayerStats] = yearRawStats.flatMap(PlayerStats.parse(_)).toDS()
    parsedPlayerStats.cache()
//    println(parsedPlayerStats.count())
    //1.2 计算每一年的9个指标的平均值和标准差
    parsedPlayerStats.printSchema()

    import org.apache.spark.sql.functions._
    // 2016 fgp_avg ftp_avg ..... tp_stddev trb_stddev
    spark.conf.set("spark.sql.shuffle.partitions",4)

    val aggStats:DataFrame = parsedPlayerStats.select($"year",$"rawStats.FGP".as("fgp"),$"rawStats.FTP".as("ftp"),
    $"rawStats.threeP".as("tp"),$"rawStats.TRB".as("trb"),$"rawStats.AST".as("ast"),$"rawStats.STL".as("stl"),
      $"rawStats.BLK".as("blk"),$"rawStats.TOV".as("tov"),$"rawStats.PTS".as("pts")).groupBy($"year")
        .agg(
          avg($"fgp").as("fgp_avg"),avg($"ftp").as("ftp_avg"),
          avg($"tp").as("tp_avg"),avg($"trb").as("trb_avg"),
          avg($"ast").as("ast_avg"),avg($"stl").as("stl_avg"),
          avg($"blk").as("blk_avg"),avg($"tov").as("tov_avg"),
          avg($"pts").as("pts_avg"),
          stddev($"tp").as("tp_stddev"),stddev($"trb").as("trb_stddev"),
          stddev($"ast").as("ast_stddev"),stddev($"stl").as("stl_stddev"),
          stddev($"blk").as("blk_stddev"),stddev($"tov").as("tov_stddev"),
          stddev($"pts").as("pts_stddev"))

    def row2Map(statsDF:DataFrame) = {
      statsDF.collect().map {row =>
        val year = row.getAs[Int]("year")
        val valueMap = row.getValuesMap[Double](row.schema.map(_.name).filterNot(_.equals("year")))
        valueMap.map{case(key,value) => (s"${year}_${key}",value)
        }
      }.reduce(_ ++ _)
    }

    //可以把上面的数据组织成Map
    // Map(2016_pts_stddev -> 3.098, 2016_tov_stddev -> 3.098, 2016_blk_stddev -> 3.098, 2016_tov_avg -> 3.098,
    // 2015_pts_stddev -> 3.098, 2015_blk_avg -> 3.098, 2015_ast_avg -> 3.098, 2014_pts_stddev -> 3.098)
    val aggStatsMap:Map[String,Double] = row2Map(aggStats)
    //println(aggStatsMap)
    val aggStatsMapB = sc.broadcast(aggStatsMap)

    //2.计算每一个球员的每年的每个指标zscore以及zcore的总值
    val statsWithZScore:Dataset[PlayerStats] = parsedPlayerStats.map(PlayerStats.calculateZScore(_,aggStatsMapB.value))
    statsWithZScore.show(truncate = false)

    //3、计算标准化的zscore
    spark.conf.set("spark.sql.shuffle,partitions",4)
    val zStats = statsWithZScore.select($"year",$"zScoreStats.FGP".as("fgw"),$"zScoreStats.FTP".as("ftw"),
    $"zScoreStats.threeP".as("tp"),$"zScoreStats.TRB".as("trb"),
    $"zScoreStats.AST".as("ast"),$"zScoreStats.BLK".as("blk"),
    $"zScoreStats.STL".as("stl"),$"zScoreStats.TOV".as("tov"),
    $"zScoreStats.PTS".as("pts")).groupBy($"year")
        .agg(
          avg($"fgw").as("fgw_avg"), avg($"ftw").as("ftw_avg"),
          stddev($"fgw").as("fgw_stddev"), stddev($"ftw").as("ftw_stddev"),
          min($"fgw").as("fgw_min"), min($"ftw").as("ftw_min"),
          max($"fgw").as("fgw_max"), max($"ftw").as("ftw_max"),
          min($"tp").as("tp_min"), min($"trb").as("trb_min"),
          min($"ast").as("ast_min"), min($"blk").as("blk_min"),
          min($"stl").as("stl_min"), min($"tov").as("tov_min"),
          min($"pts").as("pts_min"), max($"tp").as("tp_max"),
          max($"trb").as("trb_max"), max($"ast").as("ast_max"),
          max($"blk").as("blk_max"), max($"stl").as("stl_max"),
          max($"tov").as("tov_max"), max($"pts").as("pts_max"))

    val zStatsMap:Map[String,Double] = row2Map(zStats)
    val zStatsMapB = sc.broadcast(zStatsMap)

    val statsWithNZScore = statsWithZScore.map(PlayerStats.calculateNZScore(_,zStatsMapB.value))
    statsWithNZScore.printSchema()

    //statsWithNZScore.show(truncate = false)

    //4、计算每一个球员的每年的经验值(等于球员从事篮球比赛的年份)
    // 1980年开始自己职业生涯(NBA) --> 20岁
    // 2000年， --> 40岁
    // 经验值：40 - 20 = 20

    //4.1：将schema打平，可以用schema + RDD[Row]
    val schemaN = StructType(
      StructField("name", StringType, true) ::
        StructField("year", IntegerType, true) ::
        StructField("age", IntegerType, true) ::
        StructField("position", StringType, true) ::
        StructField("team", StringType, true) ::
        StructField("GP", IntegerType, true) ::
        StructField("GS", IntegerType, true) ::
        StructField("MP", DoubleType, true) ::
        StructField("FG", DoubleType, true) ::
        StructField("FGA", DoubleType, true) ::
        StructField("FGP", DoubleType, true) ::
        StructField("3P", DoubleType, true) ::
        StructField("3PA", DoubleType, true) ::
        StructField("3PP", DoubleType, true) ::
        StructField("2P", DoubleType, true) ::
        StructField("2PA", DoubleType, true) ::
        StructField("2PP", DoubleType, true) ::
        StructField("eFG", DoubleType, true) ::
        StructField("FT", DoubleType, true) ::
        StructField("FTA", DoubleType, true) ::
        StructField("FTP", DoubleType, true) ::
        StructField("ORB", DoubleType, true) ::
        StructField("DRB", DoubleType, true) ::
        StructField("TRB", DoubleType, true) ::
        StructField("AST", DoubleType, true) ::
        StructField("STL", DoubleType, true) ::
        StructField("BLK", DoubleType, true) ::
        StructField("TOV", DoubleType, true) ::
        StructField("PF", DoubleType, true) ::
        StructField("PTS", DoubleType, true) ::
        StructField("zFG", DoubleType, true) ::
        StructField("zFT", DoubleType, true) ::
        StructField("z3P", DoubleType, true) ::
        StructField("zTRB", DoubleType, true) ::
        StructField("zAST", DoubleType, true) ::
        StructField("zSTL", DoubleType, true) ::
        StructField("zBLK", DoubleType, true) ::
        StructField("zTOV", DoubleType, true) ::
        StructField("zPTS", DoubleType, true) ::
        StructField("zTOT", DoubleType, true) ::
        StructField("nFG", DoubleType, true) ::
        StructField("nFT", DoubleType, true) ::
        StructField("n3P", DoubleType, true) ::
        StructField("nTRB", DoubleType, true) ::
        StructField("nAST", DoubleType, true) ::
        StructField("nSTL", DoubleType, true) ::
        StructField("nBLK", DoubleType, true) ::
        StructField("nTOV", DoubleType, true) ::
        StructField("nPTS", DoubleType, true) ::
        StructField("nTOT", DoubleType, true) :: Nil
    )

    val playerRowRDD = statsWithNZScore.rdd.map { player =>
      Row.fromSeq(Array(player.name, player.year, player.age, player.position,
        player.team, player.GP, player.GS, player.MP)
        ++ player.rawStats.productIterator.map(_.asInstanceOf[Double])
        ++ player.zScoreStats.get.productIterator.map(_.asInstanceOf[Double]) ++ Array(player.totalZScores)
        ++ player.nzScoreStats.get.productIterator.map(_.asInstanceOf[Double]) ++ Array(player.totalNZScore))
    }

    val playerDF = spark.createDataFrame(playerRowRDD,schemaN)

    //4.2 求每个球员的经验值
    playerDF.createOrReplaceTempView("player")
    spark.conf.set("spark.sql.shuffle.partitions", 4)
    val playerStatsZ = spark.sql("select (p.age-t.min_age) as experience,p.* from player as p " +
      " join (select name,min(age) as min_age from player group by name) as t on p.name=t.name")

    //5、结果我们写到文件中
    playerStatsZ.write.mode(SaveMode.Overwrite).csv("hive-practise/5-spark-nba-player/data/nba/playerStatsZ")

//    playerStatsZ.write.mode(SaveMode.Overwrite).saveAsTable(s"${db}.player")

    spark.stop()
  }

}
