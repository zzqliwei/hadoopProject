package com.westar.model;

import java.util.Map;

public class Feature {
    private String layerName;
    private Map<String, Object> attributes;
    private String polygon;

    public String getLayerName() {
        return layerName;
    }

    public void setLayerName(String layerName) {
        this.layerName = layerName;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public String getPolygon() {
        return polygon;
    }

    public void setPolygon(String polygon) {
        this.polygon = polygon;
    }

    @Override
    public String toString() {
        return "Feature{" +
                "layerName='" + layerName + '\'' +
                ", attributes=" + attributes +
                ", polygon='" + polygon + '\'' +
                '}';
    }
}
