package com.westar.dao.impl;

import com.westar.dao.TopTenDao;
import com.westar.model.TopTenMovie;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Repository("jdbcTemplateTopTenDaoImpl")
public class JdbcTemplateTopTenDaoImpl implements TopTenDao {
    @Autowired
    private JdbcTemplate hiveJdbcTemplate;

    @Override
    public List<TopTenMovie> findTopTenMoviesByYear(int year) throws SQLException {
        //hiveJdbcTemplate.execute("SET mapreduce.framework.name=local");
        String hiveSql = "SELECT /*+ MAPJOIN(movie_links)*/  m.movieId, m.moviename, l.url, m.commentscore " +
                "from movie m join movie_links l on m.movieId = l.movieId " +
                "where year=" + year + " " +
                "order by commentscore desc limit 10";

        String impalaSql = "SELECT  m.movieId, m.moviename, l.url, m.commentscore " +
                "from movie m join movie_links l on m.movieId = l.movieId " +
                "where year=" + year + " " +
                "order by commentscore desc limit 10";

        return hiveJdbcTemplate.query(impalaSql, new RowMapper<TopTenMovie>() {
            @Override
            public TopTenMovie mapRow(ResultSet resultSet, int i) throws SQLException {
                TopTenMovie dto = new TopTenMovie();
                dto.setMovieId(resultSet.getString(1));
                dto.setMovieName(resultSet.getString(2));
                dto.setUrl(resultSet.getString(3));
                dto.setCommentScore(resultSet.getFloat(4));
                return dto;
            }
        });

    }
}
