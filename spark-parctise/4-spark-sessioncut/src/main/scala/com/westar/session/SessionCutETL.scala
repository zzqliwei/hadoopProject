package com.westar.session

import java.util.concurrent.TimeUnit

import com.westar.spark.session.TrackerSession
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
 * 会话切割项目的程序入口
 * spark-submit  --class com.twq.session.SessionCutETL  \
	*--master spark://master:7077 \
  *--deploy-mode client \
	*--driver-memory 1g \
  *--executor-memory 1g \
  *--executor-cores 1 \
  *--total-executor-cores 2 \
  *--jars parquet-avro-1.8.1.jar \
	*--conf spark.sessioncut.visitLogsInputPath=hdfs://master:9999/user/hadoop/example/rawdata/visit_log.txt \
  *--conf spark.sessioncut.cookieLabelInputPath=hdfs://master:9999/user/hadoop/example/cookie_label.txt \
  *--conf spark.sessioncut.baseOutputPath=hdfs://master:9999/user/hadoop/example/output \
  *spark-sessioncut-1.0-SNAPSHOT.jar text
*或者用：
*spark-submit  --class com.twq.session.SessionCutETL  \
	*--master spark://master:7077 \
  *--deploy-mode client \
	*--driver-memory 1g \
  *--executor-memory 1g \
  *--executor-cores 1 \
  *--total-executor-cores 2 \
	*--conf spark.sessioncut.visitLogsInputPath=hdfs://master:9999/user/hadoop/example/rawdata/visit_log.txt \
  *--conf spark.sessioncut.cookieLabelInputPath=hdfs://master:9999/user/hadoop/example/cookie_label.txt \
  *--conf spark.sessioncut.baseOutputPath=hdfs://master:9999/user/hadoop/example/output \
  *spark-sessioncut-1.0-SNAPSHOT-jar-with-dependencies.jar parquet
 */
object SessionCutETL {

  private val logTypeSet = Set("pageview", "click")

  private val logger = LoggerFactory.getLogger(SessionCutETL.getClass)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    if(!conf.contains("spark.master")){
      conf.setMaster("local[*]")
    }
    conf.setAppName("SessionCutETL")

    // 开启kryo序列化
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    val basePath = "spark-parctise/4-spark-sessioncut/"

    // 通过配置拿到我们配置的输入和输出路径
    val visitLogsInputPath = conf.get("spark.sessioncut.visitLogsInputPath", s"${basePath}data/rawdata/visit_log.txt")
    val cookieLabelInputPath = conf.get("spark.sessioncut.cookieLabelInputPath", s"${basePath}data/cookie_label.txt")
    val baseOutputPath = conf.get("spark.sessioncut.baseOutputPath", s"${basePath}data/output")

    val outputFileType = if (args.nonEmpty) args(0) else "text"

    logger.info(
      s"""Starting SessionCutETL with visitLogsInputPath : ${visitLogsInputPath}
         |and cookieLabelInputPath : ${cookieLabelInputPath}
         |and baseOutputPath : ${baseOutputPath} and outputFileType : ${outputFileType}""".stripMargin)


    //网站域名标签map，可以放在数据库中，然后从数据库中捞取出来
    val domainLabelMap = Map("www.baidu.com" -> "level1", "www.ali.com" -> "level2",
      "jd.com" -> "level3", "youku.com" -> "level4")

    val domainLabelMapB = sc.broadcast(domainLabelMap)

    // 统计会话的个数
    val sessionCountAcc = sc.longAccumulator("session count")

    // 1. 加载数据(visit_log.txt)
    val rawRDD = sc.textFile(visitLogsInputPath)
    // 2. 解析rawRDD中的每一行原始日志
    // 会报序列化的错，简单的处理方式，就是TrackerLog实现 java.io.Serializable
    val parsedLogRDD = rawRDD.flatMap(RawLogParser.parse(_))
      .filter(log =>logTypeSet.contains(log.getLogType.toString))

    // 缓存 parsedLogRDD ，因为parsedLogRDD多次被用
    parsedLogRDD.persist(StorageLevel.MEMORY_AND_DISK)

    // 3. 按照cookie进行分组
    val userGroupedRDD = parsedLogRDD.groupBy(log => log.getCookie.toString)

    // 4.对每一个user(即每一个cookie)的所有的日志进行会话切割
    val sessionRDD = userGroupedRDD.flatMapValues{ case iter =>
      // user级别的日志处理
      val processor = new OneUserTrackerLogsProcessor(iter.toArray)
      processor.buildSessions(domainLabelMapB.value,sessionCountAcc)
    }

    // 5. 给会话的cookie打上标签
    val cookieLabelRDD = sc.textFile(cookieLabelInputPath).map{ case line =>
      val temp = line.split("\\|")
      (temp(0), temp(1)) // (cookie, cookie_label)
    }

    val joinRDD:RDD[(String,(TrackerSession,Option[String]))] = sessionRDD.leftOuterJoin(cookieLabelRDD)
    val cookieLabeledSessionRDD = joinRDD.map{ case (_,(session, cookieLabelOpt)) =>
        if(cookieLabelOpt.nonEmpty){
          session.setCookieLabel(cookieLabelOpt.get)
        }else{
          session.setCookieLabel("-")
        }
      session
    }

    // 6. 输出结果数据
    logger.info("starting output result data")
    OutputComponent.fromOutPutFileType(outputFileType)
      .writeOutputData(sc,baseOutputPath,parsedLogRDD,cookieLabeledSessionRDD)

    logger.info("end output result data")

    TimeUnit.SECONDS.sleep(300)

    sc.stop()

    logger.info("end SessionCutETL")
  }

}
