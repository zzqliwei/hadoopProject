package com.westar.hbase.rowkey;

import org.apache.hadoop.hbase.util.MD5Hash;

public class Test {
    public static void main(String[] args) {

        System.out.println(MD5Hash.getMD5AsHex("boo0001".getBytes()));
        System.out.println(MD5Hash.getMD5AsHex("boo0002".getBytes()));
        System.out.println(MD5Hash.getMD5AsHex("boo0003".getBytes()));
        System.out.println(MD5Hash.getMD5AsHex("boo0004".getBytes()));

        System.out.println(System.currentTimeMillis());

        System.out.println(new StringBuilder("1524536830360").reverse());
        System.out.println(new StringBuilder("1524536830362").reverse());
        System.out.println(new StringBuilder("1524536830376").reverse());

    }
}
