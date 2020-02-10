package com.westar.service;

import com.westar.model.Tile;

import java.util.List;

public interface MapTileService {
    /**
     *  根据level以及切片的名称查询切片的信息
     * @param level
     * @param tileNames
     * @return
     */
    public List<Tile> findTilesByTileName(int level, List<String> tileNames);

    public List<Tile> findTilesByLevel(int level);

}
