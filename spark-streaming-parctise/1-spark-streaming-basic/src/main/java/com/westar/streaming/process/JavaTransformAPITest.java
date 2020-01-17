package com.westar.streaming.process;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class JavaTransformAPITest {
    public static void main(String[] args) {
        // StreamingContext 编程入口
        JavaStreamingContext ssc = new JavaStreamingContext("local[2]", "JavaLocalNetworkWordCount", Durations.seconds(1),
                System.getenv("SPARK_HOME"), JavaStreamingContext.jarOfClass(JavaTransformAPITest.class.getClass()));
        //数据接收器(Receiver)
        //创建一个接收器(JavaReceiverInputDStream)，这个接收器接收一台机器上的某个端口通过socket发送过来的数据并处理
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9998, StorageLevels.MEMORY_AND_DISK_SER);

        JavaPairDStream<String,String> kvs1 = lines.mapToPair(line ->{
            String[] arr = line.split(" ");
            return new Tuple2<>(arr[0], arr[1]);
        });

        String path = "hdfs://master:9999/user/hadoop-twq/spark-course/streaming/keyvalue.txt";

        JavaPairRDD<String, String> keyvalueRDD = ssc.sparkContext().textFile(path).mapToPair(line -> {
            String[] arr = line.split(" ");
            return new Tuple2<>(arr[0], arr[1]);
        });

        //注意，如果lamda函数返回的是key value类型的DStream的话，需要用transformToPair API
        kvs1.transformToPair(rdd -> rdd.join(keyvalueRDD)).print();
        //否则的话使用transform API
        kvs1.transform(rdd -> rdd.join(keyvalueRDD).map(tu -> tu._2()._2()));
    }
}
