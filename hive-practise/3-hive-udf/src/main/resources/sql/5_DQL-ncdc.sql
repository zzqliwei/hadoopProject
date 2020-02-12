CREATE DATABASE ncdc;
USE ncdc;

--MapReduce解析并且join数据，得到parquet -->  将数据导入Hive表中 --> 基于Hive表进行数据分析
CREATE TABLE `ncdc_joined`(
`stationId` string,
`stationName` string,
`stationCity` string,
`stationState` string,
`stationICAO` string,
`stationLatitude` string,
`stationLongitude` string,
`stationElev` string,
`stationBeginTime` string,
`stationEndTime` string,
`month` string,
`day` string,
`meantemp` double,
`meantempcount` int,
`meandewpointtemp` double,
`meandewpointtempcount` int,
`meansealevelpressure` double,
`meansealevelpressurecount` int,
`meanstationpressure` double,
`meanstationpressurecount` int,
`meanvisibility` double,
`meanvisibilitycount` int,
`meanwindspeed` double,
`meanwindspeedcount` int,
`maxsustainedwindspeed` double,
`maxgustwindspeed` double,
`maxtemp` double,
`maxtempflag` string,
`mintemp` double,
`mintempflag` string,
`totalprecipitation` double,
`totalprecipitationflag` string,
`snowdepth` double,
`hasfog` boolean,
`hashail` boolean,
`hasrain` boolean,
`hassnow` boolean,
`hasthunder` boolean,
`hastornado` boolean
)
PARTITIONED BY (year STRING)
STORED AS TEXTFILE ;

CREATE EXTERNAL TABLE `ncdc_joined_temp`(
`stationId` string,
`stationName` string,
`stationCity` string,
`stationState` string,
`stationICAO` string,
`stationLatitude` string,
`stationLongitude` string,
`stationElev` string,
`stationBeginTime` string,
`stationEndTime` string,
`year` string,
`month` string,
`day` string,
`meantemp` double,
`meantempcount` int,
`meandewpointtemp` double,
`meandewpointtempcount` int,
`meansealevelpressure` double,
`meansealevelpressurecount` int,
`meanstationpressure` double,
`meanstationpressurecount` int,
`meanvisibility` double,
`meanvisibilitycount` int,
`meanwindspeed` double,
`meanwindspeedcount` int,
`maxsustainedwindspeed` double,
`maxgustwindspeed` double,
`maxtemp` double,
`maxtempflag` string,
`mintemp` double,
`mintempflag` string,
`totalprecipitation` double,
`totalprecipitationflag` string,
`snowdepth` double,
`hasfog` boolean,
`hashail` boolean,
`hasrain` boolean,
`hassnow` boolean,
`hasthunder` boolean,
`hastornado` boolean
)
STORED AS PARQUET
LOCATION "/user/hadoop/ncdc/parquet";

--插入数据
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE ncdc_joined PARTITION(year) SELECT stationId,stationName,stationCity,stationState,stationICAO,stationLatitude,stationLongitude,stationElev,stationBeginTime,stationEndTime,month,day,meanTemp,meanTempCount,meanDewPointTemp,meanDewPointTempCount,meanSeaLevelPressure,meanSeaLevelPressureCount,meanStationPressure,meanStationPressureCount,meanVisibility,meanVisibilityCount,meanWindSpeed,meanWindSpeedCount,maxSustainedWindSpeed,maxGustWindSpeed,maxTemp,maxTempFlag,minTemp,minTempFlag,totalPrecipitation,totalPrecipitationFlag,snowDepth,hasFog,hasRain,hasSnow,hasHail,hasThunder,hasTornado,year FROM ncdc_joined_temp;

--采用hive来进行数据的ETL
CREATE TABLE ncdc_joined_hive LIKE ncdc_joined;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE ncdc_joined_hive PARTITION(year)
SELECT s.stationId,s.stationName,s.city,s.state,s.ICAO,s.latitude,s.longitude,s.elev,s.beginTime,s.endTime, month,day,meanTemp,meanTempCount,meanDewPointTemp,meanDewPointTempCount,meanSeaLevelPressure,meanSeaLevelPressureCount,meanStationPressure,meanStationPressureCount,meanVisibility,meanVisibilityCount,meanWindSpeed,meanWindSpeedCount,maxSustainedWindSpeed,maxGustWindSpeed,maxTemp,maxTempFlag,minTemp,minTempFlag,totalPrecipitation,totalPrecipitationFlag,snowDepth,hasFog,hasRain,hasSnow,hasHail,hasThunder,hasTornado,year FROM ncdc_records as r JOIN station_metadata as s ON r.stationId = s.stationId;

--求出每一年的最高平均温度
select year,max(meanTemp)
FROM ncdc_joined
GROUP BY year;

SELECT year, max(meanTemp)
FROM ncdc_joined_spark
GROUP BY year;

SELECT year, max(meanTemp)
FROM ncdc_joined_hive
GROUP BY year;

EXPLAIN SELECT year, max(meanTemp)
        FROM ncdc.ncdc_joined
        GROUP BY year;


hive --hiveconf hive.root.logger=DEBUG,console

UPDATE VERSION SET SCHEMA_VERSION='2.3.0', VERSION_COMMENT='Set by MetaStore hadoop@192.168.1.220' where VER_ID=1;


















