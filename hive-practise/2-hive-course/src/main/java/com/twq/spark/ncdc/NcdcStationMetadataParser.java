package com.twq.spark.ncdc;

import org.apache.hadoop.io.Text;

import java.io.*;

public class NcdcStationMetadataParser {

    public static StationInfoDto fromLine(String line){
        String[] values = line.split(",");
        if("USAF".equals(values[0]) || values.length != 11){//处理头部
            return null;
        }

        StationInfoDto record = new StationInfoDto();
        record.setStationId(values[0].replace("\"", "") + "-" + values[1].replace("\"", ""));
        record.setStationName(values[2].replace("\"", ""));
        record.setCity(values[3].replace("\"", ""));
        record.setState(values[4].replace("\"", ""));
        record.setICAO(values[5].replace("\"", ""));
        record.setLatitude(values[6].replace("\"", ""));
        record.setLongitude(values[7].replace("\"", ""));
        record.setElev(values[8].replace("\"", ""));
        record.setBeginTime(values[9].replace("\"", ""));
        record.setEndTime(values[10].replace("\"", ""));

        return record;
    }
    public static StationInfoDto fromLing(Text record){
        return fromLine(record.toString());
    }

    public static void main(String[] args) throws IOException {
        String filePath ="hive-practise/2-hive-coursesrc/src/main/resources/isd-history.csv";
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
        String line = null;
        while ((line = bufferedReader.readLine())!=null ){
            fromLine(line);
        }

    }
}
