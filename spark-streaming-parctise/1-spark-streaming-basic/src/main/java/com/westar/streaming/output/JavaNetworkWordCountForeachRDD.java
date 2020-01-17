package com.westar.streaming.output;

import com.westar.streaming.JavaLocalNetworkWordCount;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.Arrays;
import java.util.regex.Pattern;

public class JavaNetworkWordCountForeachRDD {
    private static Pattern SPACE = Pattern.compile(" ");
    public static void main(String[] args) {
        // StreamingContext 编程入口
        JavaStreamingContext ssc = new JavaStreamingContext("local[2]","JavaNetworkWordCountForeachRDD",
                Durations.seconds(1),System.getenv("SPARK_HOME"),JavaStreamingContext.jarOfClass(JavaLocalNetworkWordCount.class.getClass()));

        //数据接收器(Receiver)
        //创建一个接收器(JavaReceiverInputDStream)，这个接收器接收一台机器上的某个端口通过socket发送过来的数据并处理
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("192.168.3.110",
                9998, StorageLevels.MEMORY_AND_DISK_SER);

        //数据处理(Process)
        //处理的逻辑，就是简单的进行word count

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
        JavaPairDStream<String,Integer> wordCounts =  words.mapToPair(s -> new Tuple2<>(s,1))
                .reduceByKey((i1,i2)->(i1+i2));
        //结果输出(Output)
        //将结果保存到mysql(七)
        wordCounts.foreachRDD((rdd,time) ->{
                rdd.foreachPartition(partitionRecords ->{
                    Connection conn = ConnectionPool.getConnection();
                    conn.setAutoCommit(false);
                    PreparedStatement statement = conn.prepareStatement("insert into wordcount(ts, word, count) values (?, ?, ?)");
                    int count = 0;
                    Tuple2<String, Integer> record = null;
                    while (partitionRecords.hasNext()){
                        count++;
                        record = partitionRecords.next();
                        statement.setLong(1, time.milliseconds());
                        statement.setString(2, record._1());
                        statement.setInt(3, record._2());
                        statement.addBatch();
                        if (count != 0 && count % 500 == 0) {
                            statement.executeBatch();
                            conn.commit();
                        }
                    }
                    statement.executeBatch();
                    statement.close();
                    conn.commit();
                    conn.setAutoCommit(true);
                    ConnectionPool.returnConnection(conn);
                });
        });
    }
}
