package com.westar.streaming.process;

import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class JavaJoinAPITest {
    public static void main(String[] args) throws InterruptedException {
        JavaStreamingContext ssc = new JavaStreamingContext("local[2]", "JavaLocalNetworkWordCount", Durations.seconds(1),
                System.getenv("SPARK_HOME"), JavaStreamingContext.jarOfClass(JavaJoinAPITest.class.getClass()));

        //数据接收器(Receiver)
        //创建一个接收器(JavaReceiverInputDStream)，这个接收器接收一台机器上的某个端口通过socket发送过来的数据并处理
        JavaReceiverInputDStream<String> lines1 = ssc.socketTextStream(
                "localhost", 9998, StorageLevels.MEMORY_AND_DISK_SER);

        JavaPairDStream<String, String> kvs1 = lines1.mapToPair(line -> {
            String[] arr = line.split(" ");
            return new Tuple2<>(arr[0], arr[1]);
        });

        JavaReceiverInputDStream<String> lines2 = ssc.socketTextStream(
                "localhost", 9997, StorageLevels.MEMORY_AND_DISK_SER);

        JavaPairDStream<String, String> kvs2 = lines2.mapToPair(line -> {
            String[] arr = line.split(" ");
            return new Tuple2<>(arr[0], arr[1]);
        });

        kvs1.join(kvs2).print();
        kvs1.fullOuterJoin(kvs2).print();
        kvs1.leftOuterJoin(kvs2).print();
        kvs1.rightOuterJoin(kvs2).print();


        //启动Streaming处理流
        ssc.start();

        //等待Streaming程序终止
        ssc.awaitTermination();
    }
}
