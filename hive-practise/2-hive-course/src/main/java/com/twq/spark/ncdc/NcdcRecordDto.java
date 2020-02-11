package com.twq.spark.ncdc;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NcdcRecordDto {
    //气象站标识
    private String stationId;
    //年度
    private String year;
    //月份
    private String month;
    //天
    private String day;
    //平均温度
    private double meanTemp;
    //平均温度计数
    private int meanTempCount;
    private double meanDewPointTemp;
    private int meanDewPointTempCount;
    //平均海平面高度
    private double meanSeaLevelPressure;
    //平均海平面高度计数
    private int meanSeaLevelPressureCount;
    private double meanStationPressure;
    private int meanStationPressureCount;
    //平均可见度
    private double meanVisibility;
    //平均可见度计数
    private int meanVisibilityCount;
    //平均风速
    private double meanWindSpeed;
    //平均风速计数
    private int meanWindSpeedCount;
    private double maxSustainedWindSpeed;
    private double maxGustWindSpeed;
    //最高温度
    private double maxTemp;
    //最高温度标识
    private String maxTempFlag;
    //最低温度
    private double minTemp;
    //最低温度标识
    private String minTempFlag;
    private double totalPrecipitation;
    private String totalPrecipitationFlag;
    //下雪深度
    private double snowDepth;
    //是否有雾
    private boolean hasFog;
    //实现下雨
    private boolean hasRain;
    //是否下雪
    private boolean hasSnow;
    private boolean hasHail;
    //是否打雷
    private boolean hasThunder;
    private boolean hasTornado;

    public String getStationId() {
        return stationId;
    }

    public void setStationId(String stationId) {
        this.stationId = stationId;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public double getMeanTemp() {
        return meanTemp;
    }

    public void setMeanTemp(double meanTemp) {
        this.meanTemp = meanTemp;
    }

    public int getMeanTempCount() {
        return meanTempCount;
    }

    public void setMeanTempCount(int meanTempCount) {
        this.meanTempCount = meanTempCount;
    }

    public double getMeanDewPointTemp() {
        return meanDewPointTemp;
    }

    public void setMeanDewPointTemp(double meanDewPointTemp) {
        this.meanDewPointTemp = meanDewPointTemp;
    }

    public int getMeanDewPointTempCount() {
        return meanDewPointTempCount;
    }

    public void setMeanDewPointTempCount(int meanDewPointTempCount) {
        this.meanDewPointTempCount = meanDewPointTempCount;
    }

    public double getMeanSeaLevelPressure() {
        return meanSeaLevelPressure;
    }

    public void setMeanSeaLevelPressure(double meanSeaLevelPressure) {
        this.meanSeaLevelPressure = meanSeaLevelPressure;
    }

    public int getMeanSeaLevelPressureCount() {
        return meanSeaLevelPressureCount;
    }

    public void setMeanSeaLevelPressureCount(int meanSeaLevelPressureCount) {
        this.meanSeaLevelPressureCount = meanSeaLevelPressureCount;
    }

    public double getMeanStationPressure() {
        return meanStationPressure;
    }

    public void setMeanStationPressure(double meanStationPressure) {
        this.meanStationPressure = meanStationPressure;
    }

    public int getMeanStationPressureCount() {
        return meanStationPressureCount;
    }

    public void setMeanStationPressureCount(int meanStationPressureCount) {
        this.meanStationPressureCount = meanStationPressureCount;
    }

    public double getMeanVisibility() {
        return meanVisibility;
    }

    public void setMeanVisibility(double meanVisibility) {
        this.meanVisibility = meanVisibility;
    }

    public int getMeanVisibilityCount() {
        return meanVisibilityCount;
    }

    public void setMeanVisibilityCount(int meanVisibilityCount) {
        this.meanVisibilityCount = meanVisibilityCount;
    }

    public double getMeanWindSpeed() {
        return meanWindSpeed;
    }

    public void setMeanWindSpeed(double meanWindSpeed) {
        this.meanWindSpeed = meanWindSpeed;
    }

    public int getMeanWindSpeedCount() {
        return meanWindSpeedCount;
    }

    public void setMeanWindSpeedCount(int meanWindSpeedCount) {
        this.meanWindSpeedCount = meanWindSpeedCount;
    }

    public double getMaxSustainedWindSpeed() {
        return maxSustainedWindSpeed;
    }

    public void setMaxSustainedWindSpeed(double maxSustainedWindSpeed) {
        this.maxSustainedWindSpeed = maxSustainedWindSpeed;
    }

    public double getMaxGustWindSpeed() {
        return maxGustWindSpeed;
    }

    public void setMaxGustWindSpeed(double maxGustWindSpeed) {
        this.maxGustWindSpeed = maxGustWindSpeed;
    }

    public double getMaxTemp() {
        return maxTemp;
    }

    public void setMaxTemp(double maxTemp) {
        this.maxTemp = maxTemp;
    }

    public String getMaxTempFlag() {
        return maxTempFlag;
    }

    public void setMaxTempFlag(String maxTempFlag) {
        this.maxTempFlag = maxTempFlag;
    }

    public double getMinTemp() {
        return minTemp;
    }

    public void setMinTemp(double minTemp) {
        this.minTemp = minTemp;
    }

    public String getMinTempFlag() {
        return minTempFlag;
    }

    public void setMinTempFlag(String minTempFlag) {
        this.minTempFlag = minTempFlag;
    }

    public double getTotalPrecipitation() {
        return totalPrecipitation;
    }

    public void setTotalPrecipitation(double totalPrecipitation) {
        this.totalPrecipitation = totalPrecipitation;
    }

    public String getTotalPrecipitationFlag() {
        return totalPrecipitationFlag;
    }

    public void setTotalPrecipitationFlag(String totalPrecipitationFlag) {
        this.totalPrecipitationFlag = totalPrecipitationFlag;
    }

    public double getSnowDepth() {
        return snowDepth;
    }

    public void setSnowDepth(double snowDepth) {
        this.snowDepth = snowDepth;
    }

    public boolean isHasFog() {
        return hasFog;
    }

    public void setHasFog(boolean hasFog) {
        this.hasFog = hasFog;
    }

    public boolean isHasRain() {
        return hasRain;
    }

    public void setHasRain(boolean hasRain) {
        this.hasRain = hasRain;
    }

    public boolean isHasSnow() {
        return hasSnow;
    }

    public void setHasSnow(boolean hasSnow) {
        this.hasSnow = hasSnow;
    }

    public boolean isHasHail() {
        return hasHail;
    }

    public void setHasHail(boolean hasHail) {
        this.hasHail = hasHail;
    }

    public boolean isHasThunder() {
        return hasThunder;
    }

    public void setHasThunder(boolean hasThunder) {
        this.hasThunder = hasThunder;
    }

    public boolean isHasTornado() {
        return hasTornado;
    }

    public void setHasTornado(boolean hasTornado) {
        this.hasTornado = hasTornado;
    }

}
