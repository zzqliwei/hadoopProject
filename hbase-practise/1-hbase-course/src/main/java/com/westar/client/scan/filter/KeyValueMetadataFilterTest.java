package com.westar.client.scan.filter;

import com.westar.client.admin.AdminTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * FamilyFilter
 * QualifierFilter
 * ColumnPrefixFilter
 * MultipleColumnPrefixFilter
 * ColumnRangeFilter
 */
public class KeyValueMetadataFilterTest {
    public static void main(String[] args) {
        Configuration config = HBaseConfiguration.create();
        //Add any necessary configuration files (hbase-site.xml, core-site.xml)
        config.addResource(new Path("src/main/resources/hbase-site.xml"));
        config.addResource(new Path("src/main/resources/core-site.xml"));

        try(Connection connection = ConnectionFactory.createConnection(config)){
            Table table = connection.getTable(TableName.valueOf(AdminTest.TABLE_NAME));


            //Scan scan = new Scan();

            //1、查询column family等于c的数据,
            //FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("f1")));
            //scan.setFilter(familyFilter);
            //上面的还不如使用
            //scan.addFamily(Bytes.toBytes("f1"));

            //2、查询某一行中的某一个column family中的column qualifier等于e的数据
            //Scan scan = new Scan(Bytes.toBytes("row-888"));
            //scan.addFamily(Bytes.toBytes("f1"));
            //QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("e")));
            //scan.setFilter(qualifierFilter);

            //3、查询某一行中的某一个column family中的column qualifier的前缀是e的数据
            //Scan scan = new Scan(Bytes.toBytes("row-888"));
            //scan.addFamily(Bytes.toBytes("f1"));
            //ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("e"));
            ///scan.setFilter(columnPrefixFilter);

            //4、查询某一行中的某一个column family中的column qualifier的前缀是abc或者xyz的数据
            //Scan scan = new Scan(Bytes.toBytes("row-888")); //option
            //scan.addFamily(Bytes.toBytes("f1"));//option
            //byte[][] prefixes = new byte[][] {Bytes.toBytes("abc"), Bytes.toBytes("xyz")};
            //Filter multipleColumnPrefixFilter = new MultipleColumnPrefixFilter(prefixes);
            //scan.setFilter(multipleColumnPrefixFilter);

            //5、查询某一行中的某一个column family中的column qualifier从bbbb到dddd的数据
            Scan scan = new Scan(Bytes.toBytes("row-888")); //option
            scan.addFamily(Bytes.toBytes("f1"));//option
            byte[] startColumn = Bytes.toBytes("bbbb");
            byte[] endColumn = Bytes.toBytes("dddd");
            Filter columnRangeFilter = new ColumnRangeFilter(startColumn, true, endColumn, true);
            scan.setFilter(columnRangeFilter);

            ResultScanner rs = table.getScanner(scan);
            try {
                for(Result r = rs.next(); r != null; r = rs.next()){
                    List<Cell> cells = r.listCells();
                    for(Cell cell: cells){
                        System.out.println(Bytes.toString(CellUtil.cloneRow(cell))+ "===> " +
                                Bytes.toString(CellUtil.cloneFamily(cell))+ "===> " +
                                Bytes.toString(CellUtil.cloneQualifier(cell))+ "===> " +
                                Bytes.toString(CellUtil.cloneValue(cell))
                        );

                    }
                }
            }finally {
                rs.close();
            }



        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
