package com.westar.dao;

import com.westar.model.TopTenMovie;

import java.sql.SQLException;
import java.util.List;

public interface TopTenDao {
    public List<TopTenMovie> findTopTenMoviesByYear(int year) throws SQLException;
}
