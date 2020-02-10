package com.westar.dao;

import com.westar.model.TileModel;

import java.util.List;

public interface MapTileDao {

    /**
     *  根据rowkeys从HBase查询出矢量瓦片的数据
     * @param rowKeys
     * @return
     */
    public List<TileModel> getTilesByTileName(List<String> rowKeys) throws Exception;
    /**
     * 根据level从HBase查询出矢量瓦片的数据
     * @param level
     * @return
     */
    public List<TileModel> getTilesByLevel(int level) throws Exception;

}
