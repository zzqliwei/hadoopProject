package com.westar.hbase.rowkey.salt;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class KeySalter {
    private AtomicInteger index = new AtomicInteger(0);
    private String[] prefixes = {"a", "b", "c", "d"};

    public String getRowKey(String originalKey){
        StringBuilder sb = new StringBuilder(prefixes[index.incrementAndGet() % 4]);
        sb.append("-").append(originalKey);
        return sb.toString();
    }

    public List<String> getAllRowKeys(String originalKey){
        List<String> allKeys = new ArrayList<>();
        for(String prefix:prefixes){
            StringBuilder sb = new StringBuilder(prefix);
            sb.append("-").append(originalKey);
            allKeys.add(sb.toString());
        }
        //a-boo0001
        //b-boo0001
        //c-boo0001
        //d-boo0001
        return allKeys;
    }
}
