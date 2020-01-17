package com.westar.streaming.kafka;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * <p>
 * Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>
 * <zkQuorum> is a list of one or more zookeeper servers that make quorum
 * <group> is the name of kafka consumer group
 * <topics> is a list of one or more kafka topics to consume from
 * <numThreads> is the number of threads the kafka consumer should use
 * <p>
 1、创建topic1和topic2
 bin/kafka-topics.sh --create --zookeeper master:2181 --replication-factor 3 --partitions 1 --topic topic1
 bin/kafka-topics.sh --create --zookeeper master:2181 --replication-factor 3 --partitions 1 --topic topic2
 2、查看topic
 bin/kafka-topics.sh --describe --zookeeper master:2181 --topic topic1

 3、启动Spark Streaming程序
 spark-submit --class com.twq.streaming.kafka.JavaKafkaWordCount \
 --master spark://master:7077 \
 --deploy-mode client \
 --driver-memory 512m \
 --executor-memory 512m \
 --total-executor-cores 4 \
 --executor-cores 2 \
 /home/hadoop-twq/spark-course/streaming/spark-streaming-datasource-1.0-SNAPSHOT-jar-with-dependencies.jar \
 master,slave1,slave2 my-consumer-group topic1,topic2 1

 bin/kafka-console-producer.sh --broker-list master:9092 --topic topic1
 */
public class JavaKafkaWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    private JavaKafkaWordCount() {
    }

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 4) {
            System.err.println("Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>");
            System.exit(1);
        }
        SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount");
        // Create the context with 2 seconds batch size
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

        int numThreads = Integer.parseInt(args[3]);
        Map<String, Integer> topicMap = new HashMap<>();
        String[] topics = args[2].split(",");
        for (String topic : topics) {
            topicMap.put(topic, numThreads);
        }

        int numStreams = 3;
        List<JavaPairDStream<String, String>> kafkaStreams = new ArrayList<>(numStreams);
        for (int i = 0; i < numStreams; i++) {
            kafkaStreams.add(KafkaUtils.createStream(jssc, args[0], args[1], topicMap));
        }
        JavaPairDStream<String, String> unifiedStream =
                jssc.union(kafkaStreams.get(0), kafkaStreams.subList(1, kafkaStreams.size()));

        JavaDStream<String> lines = unifiedStream.map(Tuple2::_2);

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());

        JavaPairDStream<String, Integer> wordCounts =
                words.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((i1, i2) -> i1 + i2);

        wordCounts.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
