package com.westar.client;

import com.westar.client.admin.AdminTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class BatchTest {
    public static void main(String[] args) {
        Configuration config = HBaseConfiguration.create();
        //Add any necessary configuration files (hbase-site.xml, core-site.xml)
        config.addResource(new Path("src/main/resources/hbase-site.xml"));
        config.addResource(new Path("src/main/resources/core-site.xml"));
        try(Connection connection = ConnectionFactory.createConnection(config)){
            Table table = connection.getTable(TableName.valueOf(AdminTest.TABLE_NAME));
            List<Row> rows = new ArrayList<>();

            Put put = new Put(Bytes.toBytes("row-999"));
            put.addColumn(Bytes.toBytes(AdminTest.CF_DEFAULT), Bytes.toBytes("m"), Bytes.toBytes("value-m"));
            rows.add(put);

            Get get = new Get(Bytes.toBytes("row-888"));
            rows.add(get);

            Delete delete = new Delete(Bytes.toBytes("row-887"));
            rows.add(delete);

            Object[] results = new Object[rows.size()];
            //table.batch(rows, results);

            table.batchCallback(rows, results,new Batch.Callback<Result>(){

                @Override
                public void update(byte[] rowKeyBytes, byte[] keyValueBytes, Result result) {
                    System.out.println(Bytes.toString(rowKeyBytes));
                    System.out.println(Bytes.toString(keyValueBytes));
                    System.out.println(result);
                }
            });

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
