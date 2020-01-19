package com.westar;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataGenerator {
    private static List<String> baseRowKeys = Arrays.asList("http://www.51cto.com/", "http://hbase.apache.org/");
    private static String getRowKey(int index) {
        return baseRowKeys.get(index % 2);
    }
    public static List<Put> getPuts(int num) {
        List<Put> list = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            String rowKey = getRowKey(i);
            Put put = new Put(Bytes.toBytes(rowKey + "-" + i + "-" + System.currentTimeMillis()));
            put.addColumn(Bytes.toBytes("content"), null, Bytes.toBytes("<html>...51CTO技术栈...</html>"));
            put.addColumn(Bytes.toBytes("link_url"), Bytes.toBytes("sina"), Bytes.toBytes("http://tech.sina.com.cn/"));
            put.addColumn(Bytes.toBytes("language"), null, Bytes.toBytes("chinese"));
            list.add(put);
        }
        return list;
    }
}
