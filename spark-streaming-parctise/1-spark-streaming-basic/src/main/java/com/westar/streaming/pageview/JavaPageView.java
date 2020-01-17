package com.westar.streaming.pageview;

import org.apache.spark.sql.sources.In;

import java.io.Serializable;

/**
 * Created by tangweiqun on 2018/1/18.
 */
public class JavaPageView implements Serializable {
    private String url;

    private int status;

    private int zipCode;

    private int userID;

    public static JavaPageView fromString(String line) {
        String[] parts = line.split("\t");
        JavaPageView pageView = new JavaPageView();
        pageView.setUrl(parts[0]);
        pageView.setStatus(Integer.parseInt(parts[1]));
        pageView.setZipCode(Integer.parseInt(parts[2]));
        pageView.setUserID(Integer.parseInt(parts[3]));
        return pageView;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getZipCode() {
        return zipCode;
    }

    public void setZipCode(int zipCode) {
        this.zipCode = zipCode;
    }

    public int getUserID() {
        return userID;
    }

    public void setUserID(int userID) {
        this.userID = userID;
    }
}