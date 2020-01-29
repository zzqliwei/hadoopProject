package com.westar.hbase.rowkey.salt;

import com.westar.hbase.rowkey.hash.KeyHasher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SaltingTest {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();

        try(Connection connection = ConnectionFactory.createConnection(config)){
            Table table = connection.getTable(TableName.valueOf("test_salt"));

            KeySalter keySalter = new KeySalter();
            List<String> rowkeys = Arrays.asList("boo0001", "boo0002", "boo0003", "boo0004");
            List<Put> puts = new ArrayList<>();

            for (String key : rowkeys) {
                Put put = new Put(Bytes.toBytes(keySalter.getRowKey(key)));
                put.addColumn(Bytes.toBytes("f"),null,Bytes.toBytes("value" + key));
                puts.add(put);
            }
            table.put(puts);
        }
    }
}
