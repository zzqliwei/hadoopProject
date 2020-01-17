package com.westar.streaming.pageview;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class JavaPageViewStream {
    public static void main(String[] args) throws InterruptedException {
        if(args.length != 3){
            System.err.println("Usage: JavaPageViewStream <metric> <host> <port>");
            System.err.println("<metric> must be one of pageCounts, slidingPageCounts," +
                    " errorRatePerZipCode, activeUserCount, popularUsersSeen");
            System.exit(1);
        }

        String metric = args[0];
        String host = args[1];
        int port = Integer.parseInt(args[2]);

        JavaStreamingContext ssc = new JavaStreamingContext("local[*]","JavaPageViewStream", Durations.seconds(1),
                System.getenv("SPARK_HOME"),JavaStreamingContext.jarOfClass(JavaPageViewStream.class.getClass()));

        ssc.checkpoint("/tmp/streaming");

        // 创建一个ReceiverInputDStream 用于接受socket host:port中的数据
        // 且将每一行的字符串转换成PageView对象
        JavaDStream<JavaPageView> pageViewJavaDStream = ssc.socketTextStream(host, port)
                .flatMap(line -> Arrays.asList(line.split("\n")).iterator())
                .map(line -> JavaPageView.fromString(line));

        // 1、统计每一秒中url被访问的次数
        JavaPairDStream<String,Long> pageCounts = pageViewJavaDStream.map(pv -> pv.getUrl()).countByValue();

        // 2、每隔2秒统计前10秒内每一个Url被访问的次数
        JavaPairDStream<String, Long> slidingPageCounts =
                pageViewJavaDStream.map(pv -> pv.getUrl())
                        .countByValueAndWindow(Durations.seconds(10), Durations.seconds(2));

        // 3、每隔2秒钟统计前30秒内每一个地区邮政编码的访问错误率情况(status非200的，表示是访问错误页面)
        JavaPairDStream<Integer,Iterable<Integer>> statusesPerZipCode =
                pageViewJavaDStream.window(Durations.seconds(30),Durations.seconds(2))
                .mapToPair(pv -> new Tuple2<>(pv.getZipCode(), pv.getStatus()))
                .groupByKey();
        JavaDStream<String> errorRatePerZipCode = statusesPerZipCode.map(zipCodeStatus ->{
            Integer zipCode = zipCodeStatus._1();
            Iterable<Integer> statuses = zipCodeStatus._2();
            int normalCount = 0;
            int statusesSize = 0;
            for (Integer status:statuses ){
                statusesSize ++;
                if(status == 200){
                    normalCount ++;
                }
            }
            int errorCount = statusesSize - normalCount;
            float errorRatio = Float.valueOf(errorCount) / statusesSize;
            if (errorRatio > 0.05) {
                return "%s: **%s**".format(zipCode.toString(), errorRatio);
            } else {
                return "%s: %s".format(zipCode.toString(), errorRatio);
            }
        });

        // 4、每隔2秒统计前15秒内有多少活跃用户
        JavaDStream<String> activeUserCount =
                pageViewJavaDStream.window(Durations.seconds(15),Durations.seconds(2))
                        .mapToPair(pv -> new Tuple2<>(pv.getUserID(), 1))
                        .groupByKey().count().map(cnt -> "Unique active users: " + cnt);

        JavaPairRDD<Integer, String> userList =
                ssc.sparkContext().parallelizePairs(Arrays.asList(new Tuple2<>(1, "lao tang"),
                        new Tuple2<>(2, "jeffy"),
                        new Tuple2<>(3, "katy")));
        switch (metric) {
            case "pageCounts":
                pageCounts.print();
                break;
            case "slidingPageCounts":
                slidingPageCounts.print();
                break;
            case "errorRatePerZipCode":
                errorRatePerZipCode.print();
                break;
            case "activeUserCount":
                activeUserCount.print();
                break;
            case "popularUsersSeen":
                // 5、统计每隔1秒内在已经存在的user是否为活跃用户
                pageViewJavaDStream.mapToPair(pv -> new Tuple2<>(pv.getUserID(), 1))
                        .foreachRDD((rdd, time) -> {
                            rdd.join(userList)
                                    .map(tuple -> tuple._2()._2())
                                    .take(10)
                                    .forEach(cnt -> {
                                        System.out.println("Saw user %s at time %s".format(cnt, time));
                                    });
                        });
                break;
            default:
                System.out.println("Invalid metric entered: " + metric);
        }

        ssc.start();
        ssc.awaitTermination();


    }
}
