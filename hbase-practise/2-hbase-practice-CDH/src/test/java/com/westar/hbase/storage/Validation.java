package com.westar.hbase.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class Validation {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();

        try(Connection connection = ConnectionFactory.createConnection(config)) {
            Table table = connection.getTable(TableName.valueOf("sensor"));
            Scan scan = new Scan();
            scan.setCaching(1);

            ResultScanner scanner = table.getScanner(scan);
            Result result = scanner.next();
            if (result != null && !result.isEmpty()) {
                Event event = new CellEventConvertor().cellToEvent(result.listCells().get(0), null);
                System.out.println(event.toString());
            } else {
                System.out.println("impossible to find requested cell");
            }

        }
    }
}
