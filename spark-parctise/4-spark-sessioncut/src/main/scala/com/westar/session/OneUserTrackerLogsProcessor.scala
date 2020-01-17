package com.westar.session

import java.net.URL
import java.util.UUID

import com.westar.spark.session.{TrackerLog, TrackerSession}
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer

class OneUserTrackerLogsProcessor(trackerLogs: Array[TrackerLog]) extends SessionGenerator {
  private val sortedTrackerLogs = trackerLogs.sortBy(_.getLogServerTime.toString)
  /**
  *  生成当前这个user下的所有的会话
    * @return
  */
  def buildSessions(domainLabelMap: Map[String, String],
                    sessionCountAcc: LongAccumulator): ArrayBuffer[TrackerSession] ={
    // 1、会话切割
    val cuttedSessionLogsResult = cutSessions(sortedTrackerLogs)

    // 2. 生成会话
    cuttedSessionLogsResult.map{ case sessionLogs =>
      val session = new TrackerSession()
      session.setSessionId(UUID.randomUUID().toString)
      session.setSessionServerTime(sessionLogs.head.getLogServerTime)
      session.setCookie(sessionLogs.head.getCookie)

      session.setIp(sessionLogs.head.getIp)
      val pageviewLogs = sessionLogs.filter(_.getLogType.toString.equals("pageview"))
      if (pageviewLogs.length == 0) {
        session.setLandingUrl("-")
      } else {
        session.setLandingUrl(pageviewLogs.head.getUrl)
      }

      session.setPageviewCount(pageviewLogs.length)

      val clickLogs = sessionLogs.filter(_.getLogType.toString.equals("click"))
      session.setClickCount(clickLogs.length)
      if (pageviewLogs.length == 0) {
        session.setDomain("-")
      } else {
        val url = new URL(pageviewLogs.head.getUrl.toString)
        session.setDomain(url.getHost)
      }
      val domainLabel = domainLabelMap.getOrElse(session.getDomain.toString, "-")
      session.setDomainLabel(domainLabel)

      // 统计会话的个数
      sessionCountAcc.add(1)

      session
    }
  }


}
