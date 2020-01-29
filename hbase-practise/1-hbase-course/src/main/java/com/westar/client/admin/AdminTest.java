package com.westar.client.admin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;

import java.io.IOException;

public class AdminTest {
    public static final String TABLE_NAME = "java_client";
    public static final String CF_DEFAULT = "f1";

    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        createSchemaTables(config);

        modifySchema(config);
    }

    private static void modifySchema(Configuration config) throws IOException {
        try(Connection connection = ConnectionFactory.createConnection(config)){
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(TABLE_NAME);
            if(!admin.tableExists(tableName)){
                System.out.println("Table does not exist.");
                System.exit(-1);
            }
            HTableDescriptor table = admin.getTableDescriptor(tableName);
            // Update existing table
            HColumnDescriptor newColumn = new HColumnDescriptor("NEWCF");
            newColumn.setCompactionCompressionType(Compression.Algorithm.GZ);
            newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            table.addFamily(newColumn);

            // Update existing column family
            HColumnDescriptor existingColumn = new HColumnDescriptor(CF_DEFAULT);
            existingColumn.setCompactionCompressionType(Compression.Algorithm.GZ);
            existingColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            table.modifyFamily(existingColumn);

            admin.modifyTable(tableName, table);


            // Delete an existing column family
            //admin.deleteColumn(tableName, CF_DEFAULT.getBytes("UTF-8"));


        }
    }

    private static void createSchemaTables(Configuration config) throws IOException {
        try(Connection connection = ConnectionFactory.createConnection(config)){
            Admin admin = connection.getAdmin();
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            table.addFamily(new HColumnDescriptor(CF_DEFAULT));
            createOrOverwrite(admin,table);
        }
    }

    private static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
        TableName tableName = table.getTableName();
        if(admin.tableExists(tableName)){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        admin.createTable(table);
    }
}
