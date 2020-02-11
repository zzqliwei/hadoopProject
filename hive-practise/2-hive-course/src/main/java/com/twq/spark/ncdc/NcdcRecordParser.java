package com.twq.spark.ncdc;

import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class NcdcRecordParser {

    public static NcdcRecordDto fromLine(String line){

        if ("STN".equals(line.substring(0, 3))) { //head
            return null;
        }

        NcdcRecordDto ncdcRecord = new NcdcRecordDto();
        ncdcRecord.setStationId(line.substring(0, 6) + "-" + line.substring(7, 12));
        ncdcRecord.setYear(line.substring(14, 18));
        ncdcRecord.setMonth(line.substring(14, 20));
        ncdcRecord.setDay(line.substring(14, 22));

        ncdcRecord.setMeanTemp(Double.parseDouble(line.substring(24, 30).trim()));
        ncdcRecord.setMeanTempCount(Integer.parseInt(line.substring(31, 33).trim()));
        ncdcRecord.setMeanDewPointTemp(Double.parseDouble(line.substring(35, 41).trim()));
        ncdcRecord.setMeanDewPointTempCount(Integer.parseInt(line.substring(42, 44).trim()));
        ncdcRecord.setMeanSeaLevelPressure(Double.parseDouble(line.substring(46, 52).trim()));
        ncdcRecord.setMeanSeaLevelPressureCount(Integer.parseInt(line.substring(53, 55).trim()));
        ncdcRecord.setMeanStationPressure(Double.parseDouble(line.substring(57, 63).trim()));
        ncdcRecord.setMeanStationPressureCount(Integer.parseInt(line.substring(64, 66).trim()));
        ncdcRecord.setMeanVisibility(Double.parseDouble(line.substring(68, 73).trim()));
        ncdcRecord.setMeanVisibilityCount(Integer.parseInt(line.substring(74, 76).trim()));
        ncdcRecord.setMeanWindSpeed(Double.parseDouble(line.substring(78, 83).trim()));
        ncdcRecord.setMeanWindSpeedCount(Integer.parseInt(line.substring(84, 86).trim()));
        ncdcRecord.setMaxSustainedWindSpeed(Double.parseDouble(line.substring(88, 93).trim()));
        ncdcRecord.setMaxGustWindSpeed(Double.parseDouble(line.substring(95, 100).trim()));
        ncdcRecord.setMaxTemp(Double.parseDouble(line.substring(102, 108).trim()));
        ncdcRecord.setMaxTempFlag(line.substring(108, 109).trim());
        ncdcRecord.setMinTemp(Double.parseDouble(line.substring(110, 116).trim()));
        ncdcRecord.setMinTempFlag(line.substring(116, 117));

        ncdcRecord.setTotalPrecipitation(Double.parseDouble(line.substring(118, 123).trim()));
        ncdcRecord.setTotalPrecipitationFlag(line.substring(123, 124));

        ncdcRecord.setSnowDepth(Double.parseDouble(line.substring(125, 130).trim()));

        char[] indicators = line.substring(132, 138).toCharArray();
        ncdcRecord.setHasFog(fromDigit(indicators[0]));
        ncdcRecord.setHasRain(fromDigit(indicators[1]));
        ncdcRecord.setHasSnow(fromDigit(indicators[2]));
        ncdcRecord.setHasHail(fromDigit(indicators[3]));
        ncdcRecord.setHasThunder(fromDigit(indicators[4]));
        ncdcRecord.setHasTornado(fromDigit(indicators[5]));

        return ncdcRecord;
    }

    private static boolean fromDigit(char digit) {
        if ('0' == digit) {
            return false;
        }

        return true;
    }

    public static NcdcRecordDto fromLine(Text record) {
        return fromLine(record.toString());
    }

    public static void main(String[] args) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream("src/main/resources/026480-99999-2017.op")));

        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            fromLine(line);
        }
    }
}
