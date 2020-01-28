package com.westar.streaming.process

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * checkpoint
 */
object MapWithStateAPITest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("MapWithStateAPITest")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Seconds(1))

    ssc.checkpoint("hdfs://master:9999/user/hadoop-twq/spark-course/streaming/checkpoint")

    val lines = ssc.socketTextStream("master", 9998, StorageLevel.MEMORY_AND_DISK_SER)

    val words = lines.flatMap(_.split(" "))

    val wordsDStream = words.map(x => (x, 1))
    val initialRDD = sc.parallelize(List(("dummy", 100L), ("source", 32L)))
    // currentBatchTime : 表示当前的Batch的时间
    // key: 表示需要更新状态的key
    // value: 表示当前batch的对应的key的对应的值
    // currentState: 对应key的当前的状态

    val stateSpec = StateSpec.function((currentBatchTime:Time,key:String,
      value: Option[Int], currentState: State[Long]) =>{
      val sum = value.getOrElse(0).toLong + currentState.getOption().getOrElse(0K)
      val output = (key,sum)
      if(!currentState.isTimingOut()){
        currentState.update(sum)
      }
      Some(output)
    }).initialState(initialRDD).numPartitions(2).timeout(Seconds(30))
    //timeout: 当一个key超过这个时间没有接收到数据的时候，这个key以及对应的状态会被移除掉

    val result = wordsDStream.mapWithState(stateSpec)

    result.print()

    result.stateSnapshots().print()

    //启动Streaming处理流
    ssc.start()

    ssc.stop(false)


    //等待Streaming程序终止
    ssc.awaitTermination()


  }

}
