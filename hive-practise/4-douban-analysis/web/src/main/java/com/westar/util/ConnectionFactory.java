package com.westar.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionFactory {

    public static Connection getHiveJdbcConn(){
        String driverName = "org.apache.hive.jdbc.HiveDriver";

        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException("not found hive jdbc driver");
        }

        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:hive2://slave1:10000/douban",
                    "hadoop-twq", "");
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("connect hive error");
        }
        return connection;
    }
}
