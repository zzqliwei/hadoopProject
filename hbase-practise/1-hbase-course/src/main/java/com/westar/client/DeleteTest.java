package com.westar.client;

import com.westar.client.admin.AdminTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 删除数据
 */
public class DeleteTest {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        //Add any necessary configuration files (hbase-site.xml, core-site.xml)
        config.addResource(new Path("src/main/resources/hbase-site.xml"));
        config.addResource(new Path("src/main/resources/core-site.xml"));

        try(Connection connection = ConnectionFactory.createConnection(config)) {
            Table table = connection.getTable(TableName.valueOf(AdminTest.TABLE_NAME));
            //1、单个删除
            singleDel(table);

            //2、批量删除
            batchDel(table);
        }


    }

    private static void batchDel(Table table) throws IOException {
        List<Delete> deletes = new ArrayList<>();

        Delete delete1 = new Delete(Bytes.toBytes("row-128"));
        delete1.addColumn(Bytes.toBytes(AdminTest.CF_DEFAULT), Bytes.toBytes("e"));
        deletes.add(delete1);

        Delete delete2 = new Delete(Bytes.toBytes("row-123"));
        delete2.addColumn(Bytes.toBytes(AdminTest.CF_DEFAULT), Bytes.toBytes("e"));
        deletes.add(delete2);

        table.delete(deletes);
    }

    /**
     *
     * @param table
     */
    private static void singleDel(Table table) throws IOException {

        Delete delete = new Delete(Bytes.toBytes("row-123"));
        delete.addColumn(Bytes.toBytes(AdminTest.CF_DEFAULT), null);

        table.delete(delete);
    }
}
