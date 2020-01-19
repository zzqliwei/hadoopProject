package com.westar.hbase.rowkey.hash;

import org.apache.hadoop.hbase.util.MD5Hash;

public class KeyHasher {
    public  static String getRowKey(String originalKey){
       return  MD5Hash.getMD5AsHex(originalKey.getBytes());
    }
}
