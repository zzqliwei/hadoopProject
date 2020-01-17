package com.westar.streaming.process;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

public class JavaReduceByKeyAndWindowAPITest {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 2) {
            System.err.println("Usage: JavaNetworkWordCount <hostname> <port>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("JavaReduceByKeyAndWindowAPITest");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        //数据接收器(Receiver)
        //创建一个接收器(JavaReceiverInputDStream)，这个接收器接收一台机器上的某个端口通过socket发送过来的数据并处理
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(args[0],Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER);

        //数据处理(Process)
        //处理的逻辑，就是简单的进行word count
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairDStream<String, Integer> windowedWordCounts =
                pairs.reduceByKeyAndWindow((i1, i2) -> (i1 + i2), Durations.seconds(30), Durations.seconds(10));

        pairs.reduceByKeyAndWindow((i1, i2) -> (i1 + i2),(i1, i2) -> (i1 - i2)
                ,Durations.seconds(30), Durations.seconds(10),2,record ->record._2() !=0 );

        //结果输出(Output)
        //将结果输出到控制台
        windowedWordCounts.print();
        ssc.start();
        ssc.awaitTermination();
    }
}
