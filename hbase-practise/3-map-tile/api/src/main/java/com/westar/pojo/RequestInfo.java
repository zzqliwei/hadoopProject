package com.westar.pojo;

import java.util.List;

public class RequestInfo {
    private int level;
    private List<String> tiles;

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public List<String> getTiles() {
        return tiles;
    }

    public void setTiles(List<String> tiles) {
        this.tiles = tiles;
    }

    @Override
    public String toString() {
        return "RequestInfo{" +
                "level=" + level +
                ", tiles=" + tiles +
                '}';
    }
}
