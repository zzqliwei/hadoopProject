package com.westar.client.version;

import com.westar.client.admin.AdminTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class VersionTest {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        //Add any necessary configuration files (hbase-site.xml, core-site.xml)
        config.addResource(new Path("src/main/resources/hbase-site.xml"));
        config.addResource(new Path("src/main/resources/core-site.xml"));

        try(Connection connection = ConnectionFactory.createConnection(config)){
            Table table =connection.getTable(TableName.valueOf(AdminTest.TABLE_NAME));

            Get get = new Get(Bytes.toBytes("row-123"));
            Result r1 = table.get(get);
            byte[] b1 = r1.getValue(Bytes.toBytes(AdminTest.CF_DEFAULT), null);//获取最近版本的数据值

            get.setMaxVersions(3);
            Result r2 = table.get(get);
            byte[] b2 = r2.getValue(Bytes.toBytes(AdminTest.CF_DEFAULT), null); //获取最近三个版本的数据值

            Put put = new Put(Bytes.toBytes("row-1000"));
            put.addColumn(Bytes.toBytes(AdminTest.CF_DEFAULT), Bytes.toBytes("d"), Bytes.toBytes("kity"));
            table.put(put); //产生的版本号是服务器端的timestamp
            put.addColumn(Bytes.toBytes(AdminTest.CF_DEFAULT), Bytes.toBytes("t"), 555, Bytes.toBytes("kity"));
            table.put(put); //指定版本插入

            /**
             * Delete: for a specific version of a column.
             • Delete column: for all versions of a column.
             • Delete family: for all columns of a particular ColumnFamily
             */
            Delete delete = new Delete(Bytes.toBytes("row-1000"));
            delete.addColumn(Bytes.toBytes(AdminTest.CF_DEFAULT), Bytes.toBytes("t"), 555);
            table.delete(delete); //指定版本删除一个column value
        }


    }
}
