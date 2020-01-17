package com.westar.streaming.output;

import com.westar.streaming.JavaLocalNetworkWordCount;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

public class JavaNetworkWordCountHDFS {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        // StreamingContext 编程入口
        JavaStreamingContext ssc = new JavaStreamingContext("local[*]","JavaNetworkWordCountHDFS", Durations.seconds(2),
                System.getenv("SPARK_HOME"),JavaStreamingContext.jarOfClass(JavaNetworkWordCountHDFS.class.getClass()));

        //数据接收器(Receiver)
        //创建一个接收器(JavaReceiverInputDStream)，这个接收器接收一台机器上的某个端口通过socket发送过来的数据并处理
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
                "192.168.3.110", 9998, StorageLevels.MEMORY_AND_DISK_SER);

        //数据处理(Process)
        //处理的逻辑，就是简单的进行word count
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
        JavaPairDStream<String,Integer> wordCounts = words.mapToPair(word ->new Tuple2<>(word,1))
                .reduceByKey((x1,x2) -> (x1 + x2));

        //以文本的格式保存到HDFS上
        wordCounts.repartition(1).mapToPair(wordCount ->{
            Text text = new Text();
            text.set(wordCount.toString());
            return new Tuple2<>(NullWritable.get(),text);

        }).saveAsHadoopFiles("hdfs://master:9999/user/hadoop-twq/spark-course/streaming/data/hadoop/wordcount","-hadoop",
                NullWritable.class,Text.class, TextOutputFormat.class);

        //注意：JAVA API没有saveAsTextFiles和saveAsObjectFiles，需要用上面的API保存到HDFS上
    }
}
