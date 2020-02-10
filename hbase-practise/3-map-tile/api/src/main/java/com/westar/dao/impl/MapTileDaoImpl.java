package com.westar.dao.impl;

import com.westar.dao.MapTileDao;
import com.westar.model.TileModel;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Repository
public class MapTileDaoImpl implements MapTileDao {

    @Autowired
    private Connection connection;

    @Override
    public List<TileModel> getTilesByTileName(List<String> rowKeys) throws Exception {
        List<TileModel> tileModels = new ArrayList<TileModel>();
        // 1、获取要查询的HBase的Table的实例
        Table mapTileTable = null;
        try {
            mapTileTable = connection.getTable(TableName.valueOf("maptile"));
            //2、组装Get请求
            List<Get> gets = new ArrayList<>();

            for (String rowKey:rowKeys){
                Get get = new Get(Bytes.toBytes(rowKey));
                gets.add(get);
            }

            //3、请求HBase的表，批量的接口，返回结果数据
            Result[] results = mapTileTable.get(gets);
            for(Result result:results){
                TileModel tileModel = new TileModel();
                Cell cell = result.listCells().get(0);
                tileModel.setRowKey(Bytes.toString(CellUtil.cloneRow(cell)));
                tileModel.setEncodedTileValue(CellUtil.cloneValue(cell));
                tileModels.add(tileModel);
            }

        } finally {
            if (mapTileTable != null) {
                mapTileTable.close();
            }
        }

        //5、直接返回结果
        return tileModels;
    }

    @Override
    public List<TileModel> getTilesByLevel(int level) throws Exception {
        List<TileModel> tileModels = new ArrayList<TileModel>();
        // 1、获取要查询的HBase的Table的实例
        Table mapTileTable = null;
        try {
            mapTileTable = connection.getTable(TableName.valueOf("maptile"));
            // 2、根据level查询，scan，需要设置过滤
            //查询出rowkey以"_level"结尾的数据记录
            RegexStringComparator comp =  new RegexStringComparator("._" + level);
            RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, comp);
            Scan scan = new Scan();
            scan.setFilter(rowFilter);
            //3、请求HBase的表，批量的接口，返回结果数据
            ResultScanner scanner = mapTileTable.getScanner(scan);
            //4、进行结果解析
            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                TileModel tileModel = new TileModel();
                Cell cell = result.listCells().get(0);
                tileModel.setRowKey(Bytes.toString(CellUtil.cloneRow(cell)));
                tileModel.setEncodedTileValue(CellUtil.cloneValue(cell));
                tileModels.add(tileModel);
            }

        } finally {
            if (mapTileTable != null) {
                mapTileTable.close();
            }
        }

        //5、直接返回结果
        return tileModels;
    }
}
