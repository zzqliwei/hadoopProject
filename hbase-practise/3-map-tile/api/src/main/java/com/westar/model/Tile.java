package com.westar.model;

import java.util.List;

public class Tile {
    private int level;
    private String startColumn;
    private String startRow;
    private List<Feature> features;

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public String getStartColumn() {
        return startColumn;
    }

    public void setStartColumn(String startColumn) {
        this.startColumn = startColumn;
    }

    public String getStartRow() {
        return startRow;
    }

    public void setStartRow(String startRow) {
        this.startRow = startRow;
    }

    public List<Feature> getFeatures() {
        return features;
    }

    public void setFeatures(List<Feature> features) {
        this.features = features;
    }

    @Override
    public String toString() {
        return "Tile{" +
                "level=" + level +
                ", startColumn='" + startColumn + '\'' +
                ", startRow='" + startRow + '\'' +
                ", features=" + features +
                '}';
    }
}
