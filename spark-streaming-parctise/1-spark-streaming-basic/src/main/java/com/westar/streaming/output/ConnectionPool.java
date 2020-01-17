package com.westar.streaming.output;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class ConnectionPool {

    private static ComboPooledDataSource dataSource = new ComboPooledDataSource();
    static {
        dataSource.setJdbcUrl("jdbc:mysql://master:3306/test");//设置连接数据库的URL
        dataSource.setUser("root");//设置连接数据库的用户名
        dataSource.setPassword("WESTAR@soft1");//设置连接数据库的密码
        dataSource.setMaxPoolSize(40);//设置连接池的最大连接数

        dataSource.setMinPoolSize(2);//设置连接池的最小连接数

        dataSource.setInitialPoolSize(10);//设置连接池的初始连接数

        dataSource.setMaxStatements(100);//设置连接池的缓存Statement的最大数
    }

    public static Connection getConnection(){
        try {
            Connection connection = dataSource.getConnection();
            return connection;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void returnConnection(Connection connection){
        if(null != connection ){
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }
}
