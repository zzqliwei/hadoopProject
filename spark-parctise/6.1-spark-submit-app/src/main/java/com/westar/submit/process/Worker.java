package com.westar.submit.process;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Worker {

    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println(args.length);

        System.out.println("the args[0] is : " + args[0]);

        //读取java opts中的-Dtest=testValue参数
        String testProp = System.getProperty("test");
        System.out.println("test property value is : " + testProp);

        //读取processBuilder中的environment的参数
        System.out.println("test env value is : " + System.getenv("testEnv"));

        /**
         * 说明：
         * 系统属性和环境变量都是名称与值之间的映射，两种机制都是用来将用户的定义的信息传递给Java进程
         * 环境变量会产生更多的全局效应，因为他们不仅对java子进程可见，而且对于定义他们的进程的所有子进程都是可见的
         * 在不同的操作系统中，他们的语义有细微的差别，比如，不区分大小写。因此环境变量会有意想不到的副作用
         * 程序中尽可能使用系统属性，环境变量应该在需要全局效应的时候使用，或者在外部接口要求使用环境变量的时候
         */

        // 从启动java的参数-cp中包含的配置文件中读入配置数据
        Properties properties = new Properties();
        properties.load(Worker.class.getClassLoader().getResourceAsStream("test.properties"));
        System.out.println("limit from test.properties is :" + properties.getProperty("limit"));

        Long sleepDuration = Long.parseLong(System.getProperty("sleepDuration", "2"));
        System.out.println("sleepDuration property value is : " + sleepDuration);
        TimeUnit.SECONDS.sleep(sleepDuration);

        ProcessBuilder otherBuilder = new ProcessBuilder(ProcessBuilderTest.buildJavaCmds(true));

        otherBuilder.redirectError(ProcessBuilder.Redirect.appendTo(new File("/Users/tangweiqun/spark-course/error.txt")));

        otherBuilder.redirectOutput(ProcessBuilder.Redirect.appendTo(new File("/Users/tangweiqun/spark-course/output.txt")));
        otherBuilder.start().waitFor();
    }
}
