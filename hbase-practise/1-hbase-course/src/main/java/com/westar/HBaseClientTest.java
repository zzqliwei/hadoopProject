package com.westar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
/**
 * create 'twq:webtable','content','language','link_url'
 */

public class HBaseClientTest {
    public static void main(String[] args) {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "slave1,slave2,slave3");

        //注意需要设置java的版本为8
        try(Connection connection = ConnectionFactory.createConnection(configuration)){
            Table table = connection.getTable(TableName.valueOf("twq:webtable"));

            table.put(DataGenerator.getPuts(100000));
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("end");
        ;
    }
}
