package com.westar.client;

import com.westar.client.admin.AdminTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 添加异步数据
 */
public class BufferedMutatorTest {
    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        try(Connection connection = ConnectionFactory.createConnection(conf)){
            BufferedMutator mutator =  connection.getBufferedMutator(TableName.valueOf(AdminTest.TABLE_NAME));
            //异步写
            List<Put> puts = new ArrayList<>();
            Put put2 = new Put(Bytes.toBytes("row-124-syc"));
            //有时间限制
            put2.addColumn(Bytes.toBytes(AdminTest.CF_DEFAULT),null,1,Bytes.toBytes("value-124"));
            put2.addColumn(Bytes.toBytes(AdminTest.CF_DEFAULT),Bytes.toBytes("e"),Bytes.toBytes("value-124-e"));
            put2.setTTL(5000);
            puts.add(put2);

            Put put3 = new Put(Bytes.toBytes("row-128-syc"));
            put3.addColumn(Bytes.toBytes(AdminTest.CF_DEFAULT), null, 1, Bytes.toBytes("value-888"));
            put3.addColumn(Bytes.toBytes(AdminTest.CF_DEFAULT), Bytes.toBytes("e"), Bytes.toBytes("value-9999-e"));
            puts.add(put3);

            mutator.mutate(puts);

            mutator.flush();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
