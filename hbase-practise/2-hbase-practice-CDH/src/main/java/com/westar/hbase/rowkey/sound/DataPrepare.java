package com.westar.hbase.rowkey.sound;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class DataPrepare {
    public static void main(String[] args) throws IOException {

        InputStream ins = DataPrepare.class.getClassLoader().getResourceAsStream("sound.txt");
        BufferedReader br = new BufferedReader(new InputStreamReader(ins));

        List<SoundInfo> soundInfos = new ArrayList<>();
        String line = null;
        while ((line = br.readLine()) != null){
            SoundInfo soundInfo = new SoundInfo();
            String[] arr = line.split("\\|");
            String rowkey = format(arr[4], 6) + arr[1] + format(arr[0], 6);
            soundInfo.setRowkey(rowkey);
            soundInfo.setName(arr[2]);
            soundInfo.setCategory(arr[3]);
            soundInfos.add(soundInfo);
        }

        Configuration config = HBaseConfiguration.create();
        try(Connection connection = ConnectionFactory.createConnection(config)){
            Table table =connection.getTable(TableName.valueOf("sound"));
            List<Put> puts = new ArrayList<>();
            for(SoundInfo soundInfo : soundInfos){
                Put put = new Put(Bytes.toBytes(soundInfo.getRowkey()));
                put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("n"),Bytes.toBytes(soundInfo.getName()));
                put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("c"),Bytes.toBytes(soundInfo.getCategory()));
                puts.add(put);
            }
            table.put(puts);
        }
    }
    public static String format(String str, int num) {
        return String.format("%0" + num + "d", Integer.parseInt(str));
    }
}
