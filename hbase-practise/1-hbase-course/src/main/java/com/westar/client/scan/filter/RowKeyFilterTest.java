package com.westar.client.scan.filter;

import com.westar.client.admin.AdminTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * BinaryComparator
 * RegexStringComparator
 * SubstringComparator
 */
public class RowKeyFilterTest {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        //Add any necessary configuration files (hbase-site.xml, core-site.xml)
        config.addResource(new Path("src/main/resources/hbase-site.xml"));
        config.addResource(new Path("src/main/resources/core-site.xml"));

        try(Connection connection = ConnectionFactory.createConnection(config)) {
            Table table = connection.getTable(TableName.valueOf(AdminTest.TABLE_NAME));
            Scan scan = new Scan();
//            //1、查询出rowkey大于等于row-888的数据
//            BinaryComparator binaryComparator = new BinaryComparator(Bytes.toBytes("row-888"));
//            RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, binaryComparator);


//            //2、查询出rowkey以888结尾的数据记录
//            RegexStringComparator comp = new RegexStringComparator(".888");
//            RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, comp);

            //3、查询出rowkey不包含字符串"128"的数据
            SubstringComparator comp = new SubstringComparator("128");
            RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, comp);
            scan.setFilter(rowFilter);

            ResultScanner rs = table.getScanner(scan);
            try {
                for (Result r = rs.next(); r != null; r = rs.next()) {
                    // process result...
                    for (Cell cell : r.listCells()) {
                        System.out.println(Bytes.toString(CellUtil.cloneRow(cell)) + "===> " +
                                Bytes.toString(CellUtil.cloneFamily(cell)) + ":" +
                                Bytes.toString(CellUtil.cloneQualifier(cell)) + "{" +
                                Bytes.toString(CellUtil.cloneValue(cell)) + "}");
                    }
                }
            } finally {
                rs.close();  // always close the ResultScanner!
            }

        }
    }
}
