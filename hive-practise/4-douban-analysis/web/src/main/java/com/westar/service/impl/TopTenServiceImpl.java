package com.westar.service.impl;

import com.westar.dao.TopTenDao;
import com.westar.model.TopTenMovie;
import com.westar.service.TopTenService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.annotation.Resources;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Service
public class TopTenServiceImpl implements TopTenService {

    @Resource(name = "jdbcTemplateTopTenDaoImpl")
    private TopTenDao topTenDao;

    @Override
    public List<TopTenMovie> getTopTenMovie(int year) {
        List<TopTenMovie> topTenMovies = new ArrayList<>();
        try {
            topTenMovies = topTenDao.findTopTenMoviesByYear(year);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("error when fetch data", e);
        }
        return topTenMovies;
    }
}
