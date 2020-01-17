package com.westar.streaming.process;

import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class JavaWindowAPITest {
    public static void main(String[] args) throws InterruptedException {
        JavaStreamingContext ssc = new JavaStreamingContext("local[2]", "JavaLocalNetworkWordCount", Durations.seconds(1),
                System.getenv("SPARK_HOME"), JavaStreamingContext.jarOfClass(JavaWindowAPITest.class.getClass()));

        //数据接收器(Receiver)
        //创建一个接收器(JavaReceiverInputDStream)，这个接收器接收一台机器上的某个端口通过socket发送过来的数据并处理
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
                "localhost", 9998, StorageLevels.MEMORY_AND_DISK_SER);

        JavaDStream<String> windowDStream = lines.window(Durations.seconds(20), Durations.seconds(2));
        ssc.start();

        ssc.awaitTermination();

    }
}
