package com.westar.client;

import com.westar.client.admin.AdminTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class PutTest {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        //Add any necessary configuration files (hbase-site.xml, core-site.xml)
        config.addResource(new Path("src/main/resources/hbase-site.xml"));
        config.addResource(new Path("src/main/resources/core-site.xml"));
        try(Connection connection = ConnectionFactory.createConnection(config)){
            Table table =connection.getTable(TableName.valueOf(AdminTest.TABLE_NAME));
            //1、单个插入
            singlePut(table);

            //2、批量插入
            batchPut(table);
        }
    }

    private static void batchPut(Table table) throws IOException {
        List<Put> puts = new ArrayList<>();
        Put put2 = new Put(Bytes.toBytes("row-124"));
        put2.addColumn(Bytes.toBytes(AdminTest.CF_DEFAULT), null, 1, Bytes.toBytes("value-124"));
        put2.addColumn(Bytes.toBytes(AdminTest.CF_DEFAULT), Bytes.toBytes("e"), Bytes.toBytes("value-124-e"));
        put2.setTTL(5000);
        puts.add(put2);

        Put put3 = new Put(Bytes.toBytes("row-128"));
        put3.addColumn(Bytes.toBytes(AdminTest.CF_DEFAULT), null, 1, Bytes.toBytes("value-888"));
        put3.addColumn(Bytes.toBytes(AdminTest.CF_DEFAULT), Bytes.toBytes("e"), Bytes.toBytes("value-9999-e"));
        puts.add(put3);

        Put put4 = new Put(Bytes.toBytes("row-888"));
        put4.addColumn(Bytes.toBytes(AdminTest.CF_DEFAULT), null, 1, Bytes.toBytes("value-88833"));
        put4.addColumn(Bytes.toBytes(AdminTest.CF_DEFAULT), Bytes.toBytes("e"), Bytes.toBytes("value-222-e"));
        puts.add(put4);

        Put put5 = new Put(Bytes.toBytes("row-887"));
        put5.addColumn(Bytes.toBytes(AdminTest.CF_DEFAULT), null, 1, Bytes.toBytes("value-887"));
        put5.addColumn(Bytes.toBytes(AdminTest.CF_DEFAULT), Bytes.toBytes("e"), Bytes.toBytes("value-www-e"));
        puts.add(put5);

        table.put(puts);
    }

    private static void singlePut(Table table) throws IOException {
        Put put = new Put(Bytes.toBytes("row-123"));
        put.addColumn(Bytes.toBytes(AdminTest.CF_DEFAULT), null, 1, Bytes.toBytes("value"));
        put.addColumn(Bytes.toBytes(AdminTest.CF_DEFAULT), Bytes.toBytes("e"),  Bytes.toBytes("value-e"));
        table.put(put);
    }
}
