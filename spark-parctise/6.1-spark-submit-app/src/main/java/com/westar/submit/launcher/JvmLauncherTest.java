package com.westar.submit.launcher;

import java.util.concurrent.TimeUnit;

/**
 * Created by tangweiqun on 2017/9/15.
 *
 java -cp /Users/tangweiqun/spark/source/spark-course/spark-rdd/target/spark-rdd-1.0-SNAPSHOT.jar:/Users/tangweiqun/spark/source/spark-course/spark-submit-app/target/classes \
 -Dname=yellow -DsleepDuration=5 \
 -Xmx20M -XX:+UseParallelGC -XX:ParallelGCThreads=20 com.twq.submit.launcher.JvmLauncherTest
 */
public class JvmLauncherTest {
    public static void main(String[] args) throws InterruptedException {
        String name = System.getProperty("name");
        Long sleepDuration = Long.parseLong(System.getProperty("sleepDuration"));

//        LauncherParam launcherParam = new LauncherParam();
//        launcherParam.setName(name);
//
//        System.out.println(launcherParam);

        TimeUnit.SECONDS.sleep(sleepDuration);
    }
}
