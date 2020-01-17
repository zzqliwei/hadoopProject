package com.westar.session

import com.westar.spark.session.TrackerLog
import org.apache.commons.lang3.time.FastDateFormat

import scala.collection.mutable.ArrayBuffer

trait SessionGenerator {
  private val dataFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  /**
   * 同一个人员的不同session的集合
   * @param sortedTrackerLogs
   * @return
   */
  def cutSessions(sortedTrackerLogs: Array[TrackerLog]): ArrayBuffer[ArrayBuffer[TrackerLog]]={
    val oneCuttingSessionLogs = new ArrayBuffer[TrackerLog]() // 用于存放正在切割会话的所有的日志
    val initBuilder = ArrayBuffer.newBuilder[ArrayBuffer[TrackerLog]] // 用于存放切割完的会话所有的日志
    val cuttedSessionLogsResult = sortedTrackerLogs.foldLeft((initBuilder,Option.empty[TrackerLog])){case ((builder,preLog),currLog) =>
      val currLogTime = dataFormat.parse(currLog.getLogServerTime.toString).getTime
      // 如果当前的log与前一个log的时间超过30分钟的话，那么生成新的会话
      if (preLog.nonEmpty &&
        currLogTime - dataFormat.parse(preLog.get.getLogServerTime.toString).getTime > 30 * 60 * 1000) {
        // 切割成一个新的会话
        builder += oneCuttingSessionLogs.clone()
        oneCuttingSessionLogs.clear()
      }
      // 把当前的log放到当前的会话里面
      oneCuttingSessionLogs += currLog
      (builder, Some(currLog))
    }._1.result()

    if(oneCuttingSessionLogs.nonEmpty){
      cuttedSessionLogsResult += oneCuttingSessionLogs
    }
    cuttedSessionLogsResult
  }
}

/**
 * 按照pageview进行会话的切割
 */
trait PageViewSessionGenerator extends SessionGenerator {
  /**
   * 同一个人员的不同session的集合
   *
   * @param sortedTrackerLogs
   * @return
   */
  override def cutSessions(sortedTrackerLogs: Array[TrackerLog]): ArrayBuffer[ArrayBuffer[TrackerLog]] = {
    val oneCuttingSessionLogs = new ArrayBuffer[TrackerLog]() // 用于存放正在切割会话的所有的日志
    val initBuilder = ArrayBuffer.newBuilder[ArrayBuffer[TrackerLog]] // 用于存放切割完的会话所有的日志
    val cuttedSessionLogsResult = sortedTrackerLogs.foldLeft((initBuilder,Option.empty[TrackerLog])){case ((builder,preLog),currLog) =>

      // 如果当前的log是pageview的话，那么切割会话
      val curLogtype = currLog.getLogType.toString
      if (preLog.nonEmpty && curLogtype.equals("pageview")) {
        // 切割成一个新的会话
        builder += oneCuttingSessionLogs.clone()
        oneCuttingSessionLogs.clear()
      }
      // 把当前的log放到当前的会话里面
      oneCuttingSessionLogs += currLog
      (builder, Some(currLog))
    }._1.result()

    if(oneCuttingSessionLogs.nonEmpty){
      cuttedSessionLogsResult += oneCuttingSessionLogs
    }
    cuttedSessionLogsResult
  }
}
