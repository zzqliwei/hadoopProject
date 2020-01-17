package com.westar.streaming.flume;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;

import java.net.InetSocketAddress;

/**
 *
 * Pull-based Approach using a Custom Sink(Spark Streaming作为一个Sink存在)
 *
 * 1、将jar包scala-library_2.11.8.jar(这里一定要注意flume的classpath下是否还有其他版本的scala，要是有的话，则删掉，用这个，一般会有，因为flume依赖kafka，kafka依赖scala)、
 * commons-lang3-3.5.jar、spark-streaming-flume-sink_2.11-2.2.0.jar
 * 放置在slave2上的/home/hadoop-twq/bigdata/apache-flume-1.8.0-bin/lib下
 *
 * 2、配置/home/hadoop-twq/bigdata/apache-flume-1.8.0-bin/conf/flume-conf.properties
 *
 * 3、启动flume的agent
 * bin/flume-ng agent -n a1 -c conf -f conf/flume-conf.properties
 *
 * 4、启动Spark Streaming应用
 spark-submit --class com.twq.example.JavaFlumeEventCountPullBased \
 --master spark://master:7077 \
 --deploy-mode client \
 --driver-memory 512m \
 --executor-memory 512m \
 --total-executor-cores 4 \
 --executor-cores 2 \
 /home/hadoop-twq/spark-course/streaming/spark-streaming-datasource-1.0-SNAPSHOT-jar-with-dependencies.jar \
 slave2 44444

 3、在slave2上 telnet slave1 4545 发送消息
 */
public class JavaFlumeEventCountPullBased {
    private JavaFlumeEventCountPullBased() {
    }

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: JavaFlumeEventCountPushBased <host> <port>");
            System.exit(1);
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        Duration batchInterval = new Duration(2000);
        SparkConf sparkConf = new SparkConf().setAppName("JavaFlumeEventCountPushBased");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, batchInterval);

        JavaReceiverInputDStream<SparkFlumeEvent> flumeStream = FlumeUtils.createPollingStream(ssc, new InetSocketAddress[]{new InetSocketAddress(host, port)},
                StorageLevel.MEMORY_AND_DISK_SER_2(), 2, 2);

        flumeStream.count();

        flumeStream.count().map(in -> "Received " + in + " flume events.").print();

        ssc.start();
        ssc.awaitTermination();

    }

}
