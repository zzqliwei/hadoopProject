package com.twq.spark.ncdc;

/**
 * 气象站信息
 */
public class StationInfoDto {
    //气象站标识
    private String stationId;
    //气象站名称
    private String stationName;
    //城市
    private String city;
    private String state;
    private String ICAO;
    //经度
    private String latitude;
    //纬度
    private String longitude;
    private String elev;
    //开始时间
    private String beginTime;
    //结束时间
    private String endTime;

    public String getStationId() {
        return stationId;
    }

    public void setStationId(String stationId) {
        this.stationId = stationId;
    }

    public String getStationName() {
        return stationName;
    }

    public void setStationName(String stationName) {
        this.stationName = stationName;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getICAO() {
        return ICAO;
    }

    public void setICAO(String ICAO) {
        this.ICAO = ICAO;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

    public String getElev() {
        return elev;
    }

    public void setElev(String elev) {
        this.elev = elev;
    }

    public String getBeginTime() {
        return beginTime;
    }

    public void setBeginTime(String beginTime) {
        this.beginTime = beginTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }
}
