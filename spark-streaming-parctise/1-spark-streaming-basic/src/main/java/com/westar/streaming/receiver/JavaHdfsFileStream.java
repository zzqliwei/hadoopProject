package com.westar.streaming.receiver;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * WordCount程序，Spark Streaming消费指定目录数据的例子：
 *
 *
 * 用下面的命令在在集群中将Spark Streaming应用跑起来
 spark-submit --class com.twq.wordcount.JavaHdfsFileStream \
 --master spark://master:7077 \
 --deploy-mode client \
 --driver-memory 512m \
 --executor-memory 512m \
 --total-executor-cores 4 \
 --executor-cores 2 \
 /home/hadoop-twq/spark-course/streaming/streaming-1.0-SNAPSHOT.jar hdfs://master:9999/user/hadoop-twq/spark-course/streaming/file
 */
public class JavaHdfsFileStream {
    private static final Pattern SPACE = Pattern.compile(" ");
    public static void main(String[] args) throws InterruptedException {
        if (args.length < 1) {
            System.err.println("Usage: ForeachRDDAPI <hostname> <port>");
            System.exit(1);
        }

        // Create the context with a 1 second batch size
        SparkConf sparkConf = new SparkConf().setAppName("JavaHdfsFileStream");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        //创建一个接收器(JavaReceiverInputDStream)，这个接收器接收一台机器上的某个端口通过socket发送过来的数据并处理
        JavaDStream<String> lines = ssc.textFileStream(args[0]);

        //处理的逻辑，就是简单的进行word count
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);

        //将结果输出到控制台
        wordCounts.print();
        ssc.start();
        ssc.awaitTermination();

    }
}
