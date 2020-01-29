package com.westar.hbase.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class DataRetrieval {
    public static void main(String[] args) throws IOException, SolrServerException {
        CloudSolrClient solrClient = new CloudSolrClient.Builder()
                .withZkHost("slave3:2181,slave1:2181,slave2:2181")
                .withZkChroot("/solr").build();

        solrClient.connect();
        solrClient.setDefaultCollection("sensor");

        ModifiableSolrParams params = new  ModifiableSolrParams();
        params.set("qt", "/select");
        params.set("q", "+eventType:ALERT +partName:NE-114");

        QueryResponse response = solrClient.query(params);

        SolrDocumentList docs = response.getResults();
        if (docs.getNumFound() == 0) {return;}

        List<Get> gets = new ArrayList<>();
        docs.forEach(new Consumer<SolrDocument>() {
            @Override
            public void accept(SolrDocument entries) {
                gets.add(new Get((byte[])entries.getFieldValue("rowkey")));
            }
        });

        Configuration configuration = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(configuration);){

            Table table = connection.getTable(TableName.valueOf("sensor"));
            Result[] results = table.get(gets);
            Event event = null;
            for(Result result:results){
                if (result != null && !result.isEmpty()) {
                    while (result.advance()) {
                        event = new CellEventConvertor().cellToEvent(result.current(), event);
                        System.out.println(event.toString());
                    }
                } else {
                    System.out.println("Impossible to find requested cell");
                }

            }
        }



    }
}
