package com.westar.streaming.output

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * WordCount程序，Spark Streaming消费TCP Server发过来的实时数据的例子：
 *
 * 1、在master服务器上启动一个Netcat server
 * `$ nc -lk 9998` (如果nc命令无效的话，我们可以用yum install -y nc来安装nc)
 *
 *
 * create table wordcount(ts bigint, word varchar(50), count int);
 *
 * spark-shell --total-executor-cores 4 --executor-cores 2 --master spark://master:7077 --jars mysql-connector-java-5.1.44-bin.jar,c3p0-0.9.1.2.jar,spark-streaming-basic-1.0-SNAPSHOT.jar
 */
object NetworkWordCountForeachRDD {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("NetworkWordCountForeachRDD")
    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc,Seconds(2))
    //创建一个接收器(ReceiverInputDStream)，这个接收器接收一台机器上的某个端口通过socket发送过来的数据并处理
    val lines = ssc.socketTextStream("master", 9998, StorageLevel.MEMORY_AND_DISK_SER)

    //处理的逻辑，就是简单的进行word count
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    //将结果保存到Mysql(一)
    wordCounts.foreachRDD((rdd, time) =>{
      rdd.foreachPartition{ partitionRecords=>
        val conn = ConnectionPool.getConnection
        conn.setAutoCommit(false)
        val statement = conn.prepareStatement(s"insert into wordcount(ts, word, count) values (?, ?, ?)")
        partitionRecords.zipWithIndex.foreach{ case( (word,count),index) =>
          statement.setLong(1, time.milliseconds)
          statement.setString(2, word)
          statement.setInt(3, count)
          statement.addBatch()
          if (index != 0 && index % 500 == 0) {
            statement.executeBatch()
            conn.commit()
          }
        }
        statement.executeBatch()
        statement.close()
        conn.commit()
        conn.setAutoCommit(true)
        ConnectionPool.returnConnection(conn)
      }
      //等待Streaming程序终止
      ssc.awaitTermination()
    })

  }

}
