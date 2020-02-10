package com.westar.util;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.io.IOException;

@Configuration
public class Config {
    private static Logger LOG = LoggerFactory.getLogger(Config.class);

    @Autowired
    private Environment environment;

    @Bean
    public Connection hbaseConnn(){
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", environment.getProperty("hbase.zookeeper.quorum"));

        try {
            return ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            LOG.error("Connect to HBase error", e);
            throw new RuntimeException("Connect to HBase error", e);
        }
    }
}
