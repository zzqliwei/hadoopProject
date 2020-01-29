package com.westar.hbase.rowkey.hash;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HashingGetter {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        try(Connection connection = ConnectionFactory.createConnection(config)){
            Table table = connection.getTable(TableName.valueOf("test_hash"));

            Get get = new Get(Bytes.toBytes(KeyHasher.getRowKey("boo0001")));
            Result results = table.get(get);
            // process result...
            for (Cell cell : results.listCells()) {
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell)) + "===> " +
                        Bytes.toString(CellUtil.cloneFamily(cell)) + ":" +
                        Bytes.toString(CellUtil.cloneQualifier(cell)) + "{" +
                        Bytes.toString(CellUtil.cloneValue(cell)) + "}");
                System.out.println(cell.getTimestamp());
            }
        }
    }
}
