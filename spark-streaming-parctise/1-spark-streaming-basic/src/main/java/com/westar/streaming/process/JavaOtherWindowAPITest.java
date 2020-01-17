package com.westar.streaming.process;

import com.westar.streaming.JavaLocalNetworkWordCount;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * countByWindow
 * reduceByWindow
 * countByValueAndWindow
 */
public class JavaOtherWindowAPITest {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws InterruptedException {
        JavaStreamingContext ssc = new JavaStreamingContext("local[2]", "JavaLocalNetworkWordCount", Durations.seconds(1),
                System.getenv("SPARK_HOME"), JavaStreamingContext.jarOfClass(JavaLocalNetworkWordCount.class.getClass()));

        //数据接收器(Receiver)
        //创建一个接收器(JavaReceiverInputDStream)，这个接收器接收一台机器上的某个端口通过socket发送过来的数据并处理
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
                "localhost", 9998, StorageLevels.MEMORY_AND_DISK_SER);

        ssc.checkpoint("hdfs://master:9999/user/hadoop-twq/spark-course/streaming/checkpoint");

        //每过2秒钟，然后显示统计前10秒的数据量
        JavaDStream<Long> count = lines.countByWindow(Durations.seconds(10), Durations.seconds(2));
        count.print();

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());

        //每过2秒钟，然后用reduce func来聚合前10秒的数据
        JavaDStream<String> result =
                words.reduceByWindow((line1, line2) -> line1 + line2, Durations.seconds(10), Durations.seconds(2));
        result.print();

        //每过2秒钟，对前10秒的单词计数，相当于words.map((_, 1L)).reduceByKeyAndWindow(_ + _, _ - _)
        JavaPairDStream<String, Long> countValue = words.countByValueAndWindow(Durations.seconds(10), Durations.seconds(2));
        countValue.print();

        //启动Streaming处理流
        ssc.start();

        ssc.awaitTermination();
    }
}
