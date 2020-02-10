package com.westar.service.impl;

import com.westar.dao.MapTileDao;
import com.westar.model.Feature;
import com.westar.model.Tile;
import com.westar.model.TileModel;
import com.westar.service.MapTileService;
import no.ecc.vectortile.VectorTileDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class MapTileServiceImpl implements MapTileService {

    private static Logger LOG = LoggerFactory.getLogger(MapTileServiceImpl.class);

    @Autowired
    private MapTileDao mapTileDao;

    @Override
    public List<Tile> findTilesByTileName(int level, List<String> tileNames) {
        List<Tile> tiles = new ArrayList<Tile>();

        try{
            // 1、构建查询的RowKeys
            List<String> rowKeys = new ArrayList<String>();
            for(String tileName:tileNames){
                String reverseTileName = new StringBuilder(tileName).reverse().toString();
                rowKeys.add(reverseTileName + "_" + level);
            }
            // 2、拿着这些RowKeys去查询HBase的数据
            List<TileModel> tileModels = mapTileDao.getTilesByTileName(rowKeys);
            // 3、查到了数据之后，就需要将数据转换成List<Tile>
            tiles = transform2Tile(level,tileModels);
        }catch (Exception e){
            LOG.error("findTilesByTileName error", e);
            tiles = new ArrayList<>();
        }
        return tiles;
    }

    private List<Tile> transform2Tile(int level, List<TileModel> tileModels) throws IOException {
        List<Tile> tiles = new ArrayList<>();
        for (TileModel tileModel : tileModels) {
            Tile tile = new Tile();
            tile.setLevel(level); //设置level
            // 设置startColumn和startRow
            String rowKey = tileModel.getRowKey();
            String reverseTileName = rowKey.substring(0, rowKey.lastIndexOf("_"));
            String[] startColumnRow = new StringBuilder(reverseTileName).reverse().toString().split("_");
            tile.setStartColumn(startColumnRow[0]);
            tile.setStartRow(startColumnRow[1]);

            //设置features
            List<Feature> features = new ArrayList<>();
            VectorTileDecoder decoder = new VectorTileDecoder();
            VectorTileDecoder.FeatureIterable iterable = decoder.decode(tileModel.getEncodedTileValue());

            for (VectorTileDecoder.Feature feature : iterable) {
                Feature f = new Feature();
                f.setLayerName(feature.getLayerName());
                f.setAttributes(feature.getAttributes());
                f.setPolygon(feature.getGeometry().toText());
                features.add(f);
            }
            tile.setFeatures(features);

            tiles.add(tile);
        }
        return tiles;
    }

    @Override
    public List<Tile> findTilesByLevel(int level) {
        List<Tile> tiles = new ArrayList<Tile>();
        try{
            List<TileModel> tileModels = mapTileDao.getTilesByLevel(level);
            tiles = transform2Tile(level,tileModels);
        }catch (Exception e){
            LOG.error("findTilesByLevel error", e);
            tiles = new ArrayList<>();
        }
        return tiles;
    }
}
