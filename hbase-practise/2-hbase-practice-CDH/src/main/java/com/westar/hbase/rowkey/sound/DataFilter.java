package com.westar.hbase.rowkey.sound;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * SingleColumnValueFilter
 */
public class DataFilter {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        //Add any necessary configuration files (hbase-site.xml, core-site.xml)
        config.addResource(new Path("src/main/resources/hbase-site.xml"));
        config.addResource(new Path("src/main/resources/core-site.xml"));

        try(Connection connection = ConnectionFactory.createConnection(config)) {
            long startTime = System.currentTimeMillis();
            Table table = connection.getTable(TableName.valueOf("sound"));

            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes("00000120120901"));
            scan.setStopRow(Bytes.toBytes("00000120121001"));

            SingleColumnValueFilter nameFilter = new SingleColumnValueFilter(Bytes.toBytes("f"),Bytes.toBytes("n"),
                    CompareFilter.CompareOp.EQUAL,new SubstringComparator("中国好声音"));

            SingleColumnValueFilter categoryFilter = new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c"),
                    CompareFilter.CompareOp.EQUAL, new SubstringComparator("综艺"));

            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            filterList.addFilter(nameFilter);
            filterList.addFilter(categoryFilter);

            scan.setFilter(filterList);

            ResultScanner scanner = table.getScanner(scan);
            for(Result rs = scanner.next();rs != null;rs = scanner.next()){
                for(Cell cell : rs.listCells()) {
                    System.out.println(Bytes.toString(CellUtil.cloneRow(cell)) + "===> " +
                            Bytes.toString(CellUtil.cloneFamily(cell)) + ":" +
                            Bytes.toString(CellUtil.cloneQualifier(cell)) + "{" +
                            Bytes.toString(CellUtil.cloneValue(cell)) + "}");
                }
            }
            long endTime = System.currentTimeMillis();
            System.out.println(endTime - startTime);

        }
    }
}
