package com.westar.spark.rdd.example

import java.net.URL
import java.util.UUID

import com.esotericsoftware.kryo.Kryo
import com.westar.spark.rdd.{TrackerLog, TrackerSession}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetOutputFormat, AvroParquetWriter, AvroWriteSupport}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by tangweiqun on 2017/9/4.
 * spark-submit
 * --class com.twq.spark.rdd.example.ClickTrackerEtl
 * --master spark://master:7077
 * --deploy-mode client
 * --executor-memory 500m
 * --num-executors 2
 * --jars parquet-avro-1.8.1.jar
 * --conf spark.session.groupBy.numPartitions=2
 * spark-rdd-1.0-SNAPSHOT.jar
 */
object ClickTrackerEtl {
  private val  dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  private val sessionTimeInterval = 30 * 60 * 1000

  private val logTypeSet = Set("pageview", "click")

  def parseRawLog(line: String): TraversableOnce[TrackerLog] = {
    if(line.startsWith("#")){
      None
    }else{
      val fields = line.split("\\|")
      val trackerLog = new TrackerLog()
      trackerLog.setLogType(fields(0))
      trackerLog.setLogServerTime(fields(1))
      trackerLog.setCookie(fields(2))
      trackerLog.setIp(fields(3))
      trackerLog.setUrl(fields(4))
      Some(trackerLog)
    }
  }

  def cutSession(sortedParsedLogs: Array[TrackerLog]): ArrayBuffer[ArrayBuffer[TrackerLog]]  = {
    val sessionParsedLogsBuffer = ArrayBuffer[TrackerLog]()
    val initBuilder = ArrayBuffer.newBuilder[ArrayBuffer[TrackerLog]]
    val sessionLogResult = sortedParsedLogs.foldLeft((initBuilder,Option.empty[TrackerLog])){case ((builder, preLog),currLog)=>
      val currLogTime = dateFormat.parse(currLog.getLogServerTime.toString).getTime
        if(preLog.isDefined && currLogTime - dateFormat.parse(preLog.get.getLogServerTime.toString).getTime > sessionTimeInterval ){
          builder += sessionParsedLogsBuffer.clone()
          sessionParsedLogsBuffer.clear()
        }
      // 将当前的log加入到当前的会话中
      sessionParsedLogsBuffer += currLog
      (builder,Some(currLog))
    }._1.result()

    if(sessionParsedLogsBuffer.nonEmpty){
      sessionLogResult += sessionParsedLogsBuffer
    }
    sessionLogResult

  }

  def deleteIfExist(trackLogOutputPath: String, configuration: Configuration): Unit = {
    val path = new Path(trackLogOutputPath)
    val fs = path.getFileSystem(configuration)
    if(fs.exists(path)){
      fs.delete(path,true)
    }
  }

  def writeOutputdata(sc: SparkContext, trackerDataPath: String,
                      parsedLogRDD: RDD[TrackerLog],
                      labeledSessionRDD: RDD[TrackerSession]): Unit = {
    val configuration = sc.hadoopConfiguration
    val trackLogOutputPath = s"${trackerDataPath}/trackerLog/"

    deleteIfExist(trackLogOutputPath, configuration)
    //将trackerLog保存为parquet文件
    AvroWriteSupport.setSchema(configuration,TrackerLog.SCHEMA$)
    parsedLogRDD.map((null,_)).saveAsNewAPIHadoopFile(trackLogOutputPath,classOf[Void],
      classOf[TrackerLog],classOf[AvroParquetOutputFormat[TrackerLog]],configuration)

    val trackSessionOutputPath = s"${trackerDataPath}/trackerSession/"

    deleteIfExist(trackSessionOutputPath, configuration)

    //将TrackerSession保存为parquet文件
    AvroWriteSupport.setSchema(configuration, TrackerSession.SCHEMA$)
    labeledSessionRDD.map((null, _)).saveAsNewAPIHadoopFile(trackSessionOutputPath,
      classOf[Void], classOf[TrackerLog],
      classOf[AvroParquetOutputFormat[TrackerSession]], configuration)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ClickTrackerEtl")

    if (args.size == 0) {
      conf.setMaster("local")
    }
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator","com.westar.spark.rdd.example.ClickTrackerKryoRegistrator")

    val sc = new SparkContext(conf)

    val numPartitions = conf.getInt("spark.session.groupBy.numPartitions",10)

    val trackerDataPath =
      conf.get("spark.tracker.trackerDataPath",
        "/Users/tangweiqun/spark/source/spark-course/spark-rdd/src/main/resources")
    //网站域名标签map，可以放在数据库中，然后从数据库中捞取出来
    val domainLabelMap = Map("www.baidu.com" -> "level1", "www.ali.com" -> "level2",
      "jd.com" -> "level3", "youku.com" -> "level4")

    //将网站名称标签广播到每一台机器中
    val domainLabelMapB = sc.broadcast(domainLabelMap)

    //统计一共生成了多少个会话
    val sessionCountAccumulator = sc.longAccumulator("session count")


    // 从hdfs中获取原始日志数据
    val rawLogRDD = sc.textFile(s"${trackerDataPath}/visit_log.txt")
    // 对每一条原始日志进行解析成TrackerLog对象，并过滤掉logType不是"pageview", "click"的日志
    val parsedLogRDD = rawLogRDD.flatMap(line => parseRawLog(line))
      .filter(rawLog => logTypeSet.contains(rawLog.getLogType.toString))
      .persist(StorageLevel.DISK_ONLY)
    // 首先，对上面parsedLogRDD对cookie进行分组，得到每一个cookie下的用户访问页面和点击页面的情况
    val partitioner = new HashPartitioner(numPartitions)
    val sessionRDD = parsedLogRDD.groupBy((log: TrackerLog) => log.getCookie.toString,partitioner)
      .flatMapValues{ case iter =>
        // 对每一个cookie下的日志按照日志的时间进行升序排序
        val sortedParsedLogs = iter.toArray.sortBy( _.getLogServerTime.toString)
        // 对每一个cookie下的日志进行遍历，然后按照30分钟切割会话
        val sessionParsedLogsResult = cutSession(sortedParsedLogs)
        // 根据一个会话中的所有日志计算出一个新的会话对象
        sessionParsedLogsResult.map{ case logs =>
            val session = new TrackerSession()
          session.setSessionId(UUID.randomUUID().toString)

          session.setCookie(logs.head.getCookie)
          val clickCount = logs.filter(_.getLogType == "click").size
          session.setClickCount(clickCount)
          val pageviews = logs.filter(_.getLogType == "pageview")
          session.setPageviewCount(pageviews.size)
          val landingUrl = pageviews.head.getUrl
          session.setLandingUrl(landingUrl)

          //根据domain从广播变量中获取该domain对应的标签
          val domain = new URL(landingUrl.toString).getHost
          session.setDomain(domain)
          session.setDomainLabel(domainLabelMapB.value.getOrElse(domain.trim, "-"))

          session.setIp(logs.head.getIp)
          session.setSessionServerTime(logs.head.getLogServerTime)

          //将session累加统计器加1
          sessionCountAccumulator.add(1)
          session

        }
      }
    // 获取cookie的标签数据
    val cookieLabelRDD = sc.textFile(s"${trackerDataPath}/cookie_label.txt").map(label => {
      val records = label.split("\\|")
      (records(0), records(1))
    })
    // 给每一个session的cookie打上标签
    val labeledSessionRDD = sessionRDD.leftOuterJoin(cookieLabelRDD).map{case (cookie, (session, cookieLableOpt)) =>
      cookieLableOpt.foreach(session.setCookieLabel(_))
      session
    }

    writeOutputdata(sc, trackerDataPath, parsedLogRDD, labeledSessionRDD)
  }

}

class ClickTrackerKryoRegistrator extends KryoRegistrator{
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[TrackerLog])
    kryo.register(classOf[TrackerSession])
  }
}
