package com.westar.client;

import com.westar.client.admin.AdminTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetTest {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        //Add any necessary configuration files (hbase-site.xml, core-site.xml)
        config.addResource(new Path("src/main/resources/hbase-site.xml"));
        config.addResource(new Path("src/main/resources/core-site.xml"));
        try(Connection connection = ConnectionFactory.createConnection(config)){
            Table table = connection.getTable(TableName.valueOf(AdminTest.TABLE_NAME));
            //1、单个获取
            singleGet(table);

            //2、批量获取
            batchGet(table);
        }
    }

    private static void batchGet(Table table) throws IOException {
        List<Get> gets = new ArrayList<>();
        Get get1 = new Get(Bytes.toBytes("row-123"));
        get1.addColumn(Bytes.toBytes("f1"), null);
        gets.add(get1);

        Get get2 = new Get(Bytes.toBytes("row-128"));
        get2.setTimeStamp(1);
        gets.add(get2);

        Result[] results = table.get(gets);
        for (Result result : results) {
            for (Cell cell : result.listCells()) {
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell)) + "===> " +
                        Bytes.toString(CellUtil.cloneFamily(cell)) + ":" +
                        Bytes.toString(CellUtil.cloneQualifier(cell)) + "{" +
                        Bytes.toString(CellUtil.cloneValue(cell)) + "}");
            }
        }
    }

    private static void singleGet(Table table) throws IOException {
        Get get = new Get(Bytes.toBytes("row-123"));
        Result result = table.get(get);
        List<Cell> cells = result.listCells();
        for (Cell cell : cells){
            System.out.println(Bytes.toString(CellUtil.cloneRow(cell)) + "===> " +
                    Bytes.toString(CellUtil.cloneFamily(cell)) + ":" +
                    Bytes.toString(CellUtil.cloneQualifier(cell)) + "{" +
                    Bytes.toString(CellUtil.cloneValue(cell)) + "}");

            System.out.println(cell.getTimestamp());
        }
    }
}
