package com.westar.spark.rdd;

/**
 * Created by tangweiqun on 2017/8/28.
 */
public class 人 {

    public static String DEFAULT_NAME = "default";

    private String 名字;

    private int 年龄;

    public void 设置名字(String 名字) {
        this.名字 = 名字;
    }

    public static void doSomething() {
        System.out.println("do something");
    }

    @Override
    public String toString() {
        return "名字是：" + 名字 + " 年龄是：" + 年龄;
    }

    public static void main(String[] args) {
        System.out.println(人.DEFAULT_NAME);
        人.doSomething();
    }
}
