package com.westar.submit.process;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.String.join;

public class ProcessBuilderTest {
    public static void main(String[] args) throws InterruptedException, IOException {
        //生成java命令及其需要的参数
        List<String> cmds = buildJavaCmds(false);

        //cmdStr=/Library/Java/JavaVirtualMachines/jdk1.8.0_131.jdk/Contents/Home/jre/bin/java
        // -cp /Users/tangweiqun/spark/source/spark-course/spark-submit-app/target/classes:/Users/tangweiqun/twq/conf
        // -Dtest=helloworld -DsleepDuration=5 -Xmx200M -XX:MaxPermSize=25m com.twq.submit.process.Worker test args
        String cmdStr = joinWithList(" ", cmds);

        //创建需要启动cmds的ProcessBuilder
        ProcessBuilder builder = new ProcessBuilder(cmds);
        //设置工作目录，即你需要执行的命令所在的目录，这里是设置我的java命令所在的目录
        builder.directory(new File("/Library/Java/JavaVirtualMachines/jdk1.8.0_131.jdk/Contents/Home"));

        //设置子process的错误输出的地方，这里设置成和当前的进程输出一致
        builder.redirectError(ProcessBuilder.Redirect.INHERIT);
        //builder.redirectError(ProcessBuilder.Redirect.appendTo(new File("/Users/tangweiqun/spark-course/error.txt")));

        //设置子process的输出的地方，这里设置成和当前的进程输出一致
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        //builder.redirectOutput(ProcessBuilder.Redirect.appendTo(new File("/Users/tangweiqun/spark-course/output.txt")));

        //设置子进程的启动环境变量，子进程可以通过System.getenv("testEnv")获取到testEnvValue这个值
        builder.environment().put("testEnv", "testEnvValue");

        /*for(Map.Entry<String, String> entry: builder.environment().entrySet()) {
            System.out.println("环境变量：" + entry.getKey() + " = " + entry.getValue());
        }*/

        //System.out.println(builder.directory());

        //启动子进程
        Process p =  builder.start();


        //等待子进程结束并退出
        p.waitFor();
    }

    public static List<String> buildJavaCmds(boolean isWithoutProperties) {
        List<String> cmds = new ArrayList<String>();
        //设置java命令
        String javaHome = System.getProperty("java.home");
        if (javaHome == null) {
            System.out.println("JAVA_HOME is null");
            javaHome = "/Library/Java/JavaVirtualMachines/jdk1.8.0_131.jdk/Contents/Home";
        }
        String javaCmd = join(File.separator, javaHome, "bin", "java");
        //javaCmd=/Library/Java/JavaVirtualMachines/jdk1.8.0_131.jdk/Contents/Home/jre/bin/java
        cmds.add(javaCmd);

        //设置启动该jvm需要依赖的class文件所在的地方
        List<String> classPaths = buildClasspath();
        cmds.add("-cp");
        cmds.add(joinWithList(File.pathSeparator, classPaths));

        addJavaOpts(cmds, isWithoutProperties);

        //设置启动jvm的main方法入口的类
        cmds.add("com.twq.submit.process.Worker");

        //设置main方法的参数
        cmds.add("test");
        cmds.add("args");
        return cmds;
    }

    private static void addJavaOpts(List<String> cmds, boolean isWithoutProperties) {
        //设置jvm的属性参数, 子进程可以用System.getProperty("test")获取到helloworld这个值
        if (!isWithoutProperties) {
            cmds.add("-Dtest=helloworld");
            cmds.add("-DsleepDuration=5");
        }

        //设置jvm的扩展参数
        cmds.add("-Xmx200M");
        cmds.add("-XX:MaxPermSize=25m");//java8.0中已经取消了这个参数
    }

    private static List<String> buildClasspath() {
        List<String> cps = new ArrayList<String>();
        cps.add("/Users/tangweiqun/spark/source/spark-course/spark-submit-app/target/classes");
        cps.add("/Users/tangweiqun/spark-course");
        return cps;
    }

    private static String join(String sep, String... elements) {
        return joinWithList(sep, Arrays.asList(elements));
    }

    private static String joinWithList(String sep, Iterable<String> elements) {
        StringBuilder sb = new StringBuilder();
        for (String ele: elements) {
            if (ele != null) {
                if (sb.length() > 0) {
                    sb.append(sep);
                }
            }
            sb.append(ele);
        }
        return sb.toString();
    }
}
