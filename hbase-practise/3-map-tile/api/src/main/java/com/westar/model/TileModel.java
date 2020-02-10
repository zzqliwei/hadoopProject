package com.westar.model;

import java.util.Arrays;

public class TileModel {
    private String rowKey;
    private byte[] encodedTileValue;

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public byte[] getEncodedTileValue() {
        return encodedTileValue;
    }

    public void setEncodedTileValue(byte[] encodedTileValue) {
        this.encodedTileValue = encodedTileValue;
    }

    @Override
    public String toString() {
        return "TileModel{" +
                "rowKey='" + rowKey + '\'' +
                ", encodedTileValue=" + Arrays.toString(encodedTileValue) +
                '}';
    }
}
