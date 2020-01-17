package com.westar.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;
import scala.math.Ordering;

import java.util.Arrays;
import java.util.regex.Pattern;
/**
 * WordCount程序，Spark Streaming消费TCP Server发过来的实时数据的例子：
 *
 * 1、在master服务器上启动一个Netcat server
 * `$ nc -lk 9998` (如果nc命令无效的话，我们可以用yum install -y nc来安装nc)
 *
 * 2、用下面的命令在在集群中将Spark Streaming应用跑起来
 spark-submit --class com.twq.streaming.JavaNetworkWordCount \
 --master spark://master:7077 \
 --deploy-mode client \
 --driver-memory 512m \
 --executor-memory 512m \
 --total-executor-cores 1 \
 --executor-cores 1 \
 /home/hadoop-twq/spark-course/streaming/spark-streaming-basic-1.0-SNAPSHOT.jar master 9998
 */
public class JavaNetworkWordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 2) {
            System.err.println("Usage: JavaNetworkWordCount <hostname> <port>");
            System.exit(1);
        }
        // StreamingContext 编程入口
        SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount").setMaster("local[*]");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        //数据接收器(Receiver)
        //创建一个接收器(JavaReceiverInputDStream)，这个接收器接收一台机器上的某个端口通过socket发送过来的数据并处理

        //hostname and port
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
                args[0],Integer.parseInt(args[1]), StorageLevel.MEMORY_AND_DISK());

        //数据处理(Process)
        //处理的逻辑，就是简单的进行word count
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(
                SPACE.split(x)).iterator());

        JavaPairDStream<String,Integer> wordscounts = words.mapToPair(s
        -> new Tuple2<>(s,1)) .reduceByKey((i1,i2) -> i1 + i2);

        //结果输出(Output)
        //将结果输出到控制台
        wordscounts.print();
        ssc.start();
        ssc.awaitTermination();




    }
}
