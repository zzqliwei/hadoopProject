package com.westar.client;


import com.westar.client.admin.AdminTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class SingleRowLockTest {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        //Add any necessary configuration files (hbase-site.xml, core-site.xml)
        config.addResource(new Path("src/main/resources/hbase-site.xml"));
        config.addResource(new Path("src/main/resources/core-site.xml"));
        try(Connection connectin = ConnectionFactory.createConnection(config)){
            Table table = connectin.getTable(TableName.valueOf(AdminTest.TABLE_NAME));
            //1、用于计数器，比如可以统计一个帖子的浏览量
            //如果没有Increment的话，我们应该这样做：
            /**
             *    lock=_hTable.lockRow(Bytes.toBytes(key))
             r=_hTable.get(g);
             p = r + 1
             _hTable.put(p);
             _hTable.unlockRow(lock);
             */
            Increment increment = new Increment(Bytes.toBytes("row-123"));
            increment.addColumn(Bytes.toBytes(AdminTest.CF_DEFAULT), Bytes.toBytes("i"), 0);
            table.increment(increment);

            Increment increment1 = new Increment(Bytes.toBytes("row-123"));
            increment1.addColumn(Bytes.toBytes(AdminTest.CF_DEFAULT), Bytes.toBytes("i"), 1);
            table.increment(increment1);

            //向一个已经存在的Row中追加一个Column或者多个Column的值，可以保证原子性
            Append append = new Append(Bytes.toBytes("row-123"));
            append.add(Bytes.toBytes(AdminTest.CF_DEFAULT), Bytes.toBytes("f"), Bytes.toBytes("test-append"));
            table.append(append);

            //使用mutateRow来保证同一个Row下多个put和delete操作的原子性
            Put put = new Put(Bytes.toBytes("row-123"));
            put.addColumn(Bytes.toBytes(AdminTest.CF_DEFAULT), Bytes.toBytes("c"), Bytes.toBytes("test-c"));
            Delete delete = new Delete(Bytes.toBytes("row-123"));
            delete.addColumn(Bytes.toBytes(AdminTest.CF_DEFAULT), Bytes.toBytes("i"));

            RowMutations rowMutations = new RowMutations();
            rowMutations.add(put);
            rowMutations.add(delete);
            table.mutateRow(rowMutations);

            //原子插入
            Put put1 = new Put(Bytes.toBytes("row-987"));
            put1.addColumn(Bytes.toBytes(AdminTest.CF_DEFAULT), Bytes.toBytes("m"),  Bytes.toBytes("test-check"));
            boolean result = table.checkAndPut(Bytes.toBytes("row-987")
                    ,Bytes.toBytes(AdminTest.CF_DEFAULT), Bytes.toBytes("m"),null,put1);

            //原子删除
            Delete delete1 = new Delete(Bytes.toBytes("row-987"));
            delete1.addColumn(Bytes.toBytes("row-987"), Bytes.toBytes(AdminTest.CF_DEFAULT));
            table.checkAndDelete(Bytes.toBytes("row-987"), Bytes.toBytes(AdminTest.CF_DEFAULT), Bytes.toBytes("m"), null, delete1);

        }

    }
}
