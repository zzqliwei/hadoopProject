package com.westar.dao.impl;

import com.westar.dao.TopTenDao;
import com.westar.model.TopTenMovie;
import com.westar.util.ConnectionFactory;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Repository("jdbcTopTenDaoImpl")
public class JDBCTopTenDaoImpl implements TopTenDao {
    @Override
    public List<TopTenMovie> findTopTenMoviesByYear(int year) throws SQLException {
        Connection connection = ConnectionFactory.getHiveJdbcConn();
        Statement statement = connection.createStatement();
        statement.execute("SET mapreduce.framework.name=local");
        String sql = "SELECT /*MAPJOIN(movie_links)*/ m.movieId,m.moviename,l.url,m.commentscore " +
                " from movie m join movie_links l on m.movieId=l.movieId " +
                " where year="+year+" "+
                " order by commentscore desc limit 10";
        System.out.println("Running:"+sql);
        ResultSet res = statement.executeQuery(sql);

        List<TopTenMovie> topTenMovieDtos = new ArrayList<>();
        while (res.next()) {
            TopTenMovie dto = new TopTenMovie();
            dto.setMovieId(res.getString(1));
            dto.setMovieName(res.getString(2));
            dto.setUrl(res.getString(3));
            dto.setCommentScore(res.getFloat(4));
            topTenMovieDtos.add(dto);
        }
        return topTenMovieDtos;
    }
}
