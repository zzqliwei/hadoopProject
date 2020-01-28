package com.westar.streaming.receiver

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.derby.impl.store.access.UTF
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 自定义一个Receiver，这个Receiver从socket中接收数据
 * 接收过来的数据解析成以 \n 分隔开的text
 *
 * 首先启动 Netcat server
 *    `$ nc -lk 9998`
 * 然后运行spark streaming app
 *    spark-shell --master spark://master:7077 --total-executor-cores 4 --executor-cores 2 --jars spark-streaming-basic-1.0-SNAPSHOT.jar
 *
 *    import com.twq.streaming.receiver.CustomReceiver
 */
object CustomReceiver {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("CustomReceiver")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Seconds(1))

    // 调用 receiverStream api，将自定义的Receiver传进去
    val lines = ssc.receiverStream(new CustomReceiver("master",9998))
    val words = lines.flatMap(_.split(" "))
    val wordCouts = words.map((_,1)).reduceByKey( _ + _)

    wordCouts.print()

    ssc.start()
    ssc.awaitTermination()

    ssc.stop(false)

  }

}

class CustomReceiver(host:String,port:Int)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_SER_2) with Logging{
  override def onStart(): Unit = {
    // 启动一个线程，开始接收数据
    new Thread("Socket Receiver"){



      override def run(): Unit = {
        receive()
      }
    }
  }

  override def onStop(): Unit = {

  }
  private def receive(): Unit = {
    var socket:Socket = null
    var userInput:String = null
    try{
      logInfo("Connecting to " + host + ":" + port)
      socket = new Socket(host,port)
      logInfo("Connected to " + host + ":" + port)
      val reader = new BufferedReader(
        new InputStreamReader(socket.getInputStream,StandardCharsets.UTF_8)
      )
      userInput = reader.readLine()
      while (!isStopped &&userInput !=null) {
        store(userInput)
        userInput = reader.readLine()
      }
      reader.close()
      socket.close()
      logInfo("Stopped receiving")
      restart("Trying to connect again")
    }catch {
      case e:java.net.ConnectException =>
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        restart("Error receiving data", t)
    }
  }
}
