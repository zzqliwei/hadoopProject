package com.westar.submit;

import org.apache.spark.launcher.SparkLauncher;

import java.io.File;
import java.io.IOException;

public class ProcessCodeSubmitApp {
    public static void main(String[] args) {
        try {
            Process process = new SparkLauncher()
                    .setAppResource("/Users/tangweiqun/spark/source/spark-course/spark-submit-app/target/spark-submit-app-1.0-SNAPSHOT.jar")
                    .setMainClass("com.westar.submit.LocalSparkTest")
                    .setAppName("ProcessCodeSubmitApp")
                    .setMaster("yarn")
                    .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
                    .redirectError()
                    .redirectOutput(new File("/Users/tangweiqun/spark-course/output.txt"))
                    .launch();
            System.out.println("started app");
            process.waitFor();
            System.out.println("end app");

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
