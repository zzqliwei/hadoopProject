package com.westar.client.scan;

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

public class ScanBasic {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.addResource(new Path("src/main/resources/hbase-site.xml"));
        config.addResource(new Path("src/main/resources/core-site.xml"));

        try (Connection connection = ConnectionFactory.createConnection(config)) {
            Table table = connection.getTable(TableName.valueOf(AdminTest.TABLE_NAME));

            Scan scan = new Scan();
            //1、只查询column family为f1的数据
            //scan.addFamily(Bytes.toBytes(AdminTest.CF_DEFAULT));
            //1、只查询column为f1:e的数据
            //scan.addColumn(Bytes.toBytes(AdminTest.CF_DEFAULT), Bytes.toBytes("e"));

            //2、batch和caching和hbase table column size共同决定了rpc的次数。
            //scan可以通过setCaching与setBatch方法提高速度（以空间换时间）；
            //scan.setCaching(500); //每次rpc的请求记录数，默认是1；cache大可以优化性能，但是太大了会花费很长的时间进行一次传输。
            //scan.setBatch(2); //设置每次取的column size；有些row特别大，所以需要分开传给client，就是一次传一个row的几个column。

            //3、scan可以通过setStartRow与setStopRow来限定范围（[start，end）start是闭区间，end是开区间）。范围越小，性能越高。
            scan.setStartRow(Bytes.toBytes("row-128"));
            scan.setStopRow(Bytes.toBytes("row-888"));

            ResultScanner rs = table.getScanner(scan);
            try {
                for (Result r = rs.next(); r != null; r = rs.next()) {
                    for (Cell cell: r.listCells()){
                        System.out.println(Bytes.toString(CellUtil.cloneRow(cell)) + "===> " +
                                Bytes.toString(CellUtil.cloneFamily(cell)) + ":" +
                                Bytes.toString(CellUtil.cloneQualifier(cell)) + "{" +
                                Bytes.toString(CellUtil.cloneValue(cell)) + "}");
                    }
                }
            }finally {
                rs.close();
            }


        }
    }
}
