package com.westar.service;

import com.westar.model.TopTenMovie;

import java.util.List;

public interface TopTenService {
    public List<TopTenMovie> getTopTenMovie(int year);
}
