package com.westar.submit;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class HandleCodeSubmitApp {
    public static void main(String[] args) {
        SparkAppHandle handle = null;
        try {
            System.out.println("starting app with handler");

            handle = new SparkLauncher()
                    .setAppResource("/Users/tangweiqun/spark/source/spark-course/spark-submit-app/target/spark-submit-app-1.0-SNAPSHOT.jar")
                    .setMainClass("com.westar.submit.LocalSparkTest")
                    .setAppName("ProcessCodeSubmitApp")
                    .setMaster("yarn")
                    .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
                    .redirectError()
                    .redirectOutput(new File("/Users/tangweiqun/spark-course/output.txt"))
                    .startApplication();

            handle.addListener(new SparkAppHandle.Listener() {
                @Override
                public void stateChanged(SparkAppHandle sparkAppHandle) {
                    System.out.println("stateChanged :" + sparkAppHandle.getState());
                }

                @Override
                public void infoChanged(SparkAppHandle sparkAppHandle) {
                    System.out.println("infoChanged : " + sparkAppHandle.getAppId());
                }
            });

            while(handle.getState() != SparkAppHandle.State.FINISHED){
                TimeUnit.SECONDS.sleep(5);
                System.out.println("状态为：" + handle.getState());
                System.out.println("appId为：" + handle.getAppId());
                continue;
            }
            System.out.println("end app");

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
