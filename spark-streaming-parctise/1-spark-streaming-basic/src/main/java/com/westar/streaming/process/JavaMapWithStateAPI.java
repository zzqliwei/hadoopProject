package com.westar.streaming.process;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
/**
 * Counts words cumulatively in UTF8 encoded, '\n' delimited text received from the network every
 * second starting with initial value of word count.
 * Usage: JavaMapWithStateAPI <hostname> <port>
 * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive
 * data.
 * <p>
 * To run this on your local machine, you need to first run a Netcat server
 * `$ nc -lk 9998`
 * and then run the example
 spark-submit --class com.twq.checkpoint.JavaMapWithStateAPI \
 --master spark://master:7077 \
 --deploy-mode client \
 --driver-memory 512m \
 --executor-memory 512m \
 --total-executor-cores 4 \
 --executor-cores 2 \
 /home/hadoop-twq/spark-course/streaming/streaming-1.0-SNAPSHOT.jar master 9998
 */
public class JavaMapWithStateAPI {
    public static Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 2) {
            System.err.println("Usage: JavaMapWithStateAPI <hostname> <port>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("JavaMapWithStateAPI");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        ssc.checkpoint("hdfs://master:9999/spark-streaming/mapstate");

        // Initial state RDD input to mapWithState
        List<Tuple2<String, Integer>> tuples =
                Arrays.asList(new Tuple2<>("hello", 1), new Tuple2<>("world", 1));

        JavaPairRDD<String,Integer> initialRDD = ssc.sparkContext().parallelizePairs(tuples);

        JavaReceiverInputDStream<String> lines =
                ssc.socketTextStream(args[0], Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER_2);

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());

        JavaPairDStream<String, Integer> wordsDstream = words.mapToPair(s -> new Tuple2<>(s, 1));

        // Update the cumulative count function
        Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunc =
                (word, one, state) -> {
                    int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
                    Tuple2<String, Integer> output = new Tuple2<>(word, sum);
                    state.update(sum);
                    return output;
                };

        // DStream made of get cumulative counts that get updated in every batch
        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateDstream =
                wordsDstream.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD));

        //Typically, a checkpoint interval of 5 - 10 sliding intervals of a DStream is a good setting to try.
        stateDstream.checkpoint(Durations.milliseconds(10));

        stateDstream.print();
        ssc.start();
        ssc.awaitTermination();

    }
}
