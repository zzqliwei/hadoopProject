package com.westar.dataset.datasource

import java.util.Properties

import com.westar.dataset.Utils
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * spark-shell --master spark://master:7077 --jars mysql-connector-java-5.1.44-bin.jar
 *
 * //写数据的过程：
 * //1 : 建表
 * //第一次写的时候，需要创建一张表，建表语句类似如下：
 * //CREATE TABLE t (name string) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1
 * //ENGINE=InnoDB使用innodb引擎 DEFAULT CHARSET=utf8 数据库默认编码为utf-8 AUTO_INCREMENT=1 自增键的起始序号为1
 * //.InnoDB，是MySQL的数据库引擎之一，为MySQL AB发布binary的标准之一
 * //属性配置ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1可以通过参数createTableOptions传给spark
 *
 * //2: 设置表的schema
 * // 一般表的schema是和DataFrame是一致的，字段的类型是从spark sql的DataType翻译到各个数据库对应的数据类型
 * // 如果字段在数据库中的类型不是你想要的，
 * // 你可以通过参数createTableColumnTypes来设置createTableColumnTypes=age long,name string
 *
 * //3: 事务隔离级别的设置，通过参数isolationLevel设置
 * //  NONE 不支持事物
 * // READ_UNCOMMITTED 会出现脏读、不可重复读以及幻读
 * // READ_COMMITTED 不会出现脏读，但是还是会出现不可重复读以及幻读
 * // REPEATABLE_READ  不会出现脏读以及不可重复读，但是还会出现幻读
 * // SERIALIZABLE   脏读、不可重复读以及幻读都不会出现了
 *
 * //4：写数据
 * //写数据的过程中可以采用批量写数据，每一批写的数据量的大小可以通过参数batchsize设置，默认是：1000
 *
 * //5：第二次写数据的时候，这个时候表已经存在了，所以需要区分SaveMode
 * //当SaveMode=Overwrite 的时候，需要先清理表，然后再写数据。清理表的方法又分两种：
 * //  第一种是truncate即清空表，如果是这种的话，则先清空表，然后再写数据
 * //  第二种是drop掉表，如果是这种的话，则先drop表，然后建表，最后写数据
 * //以上两种方式的选择，可以通过参数truncate(默认是false)控制。因为truncate清空数据可能会失败，所以可以使用drop table的方式
 * //而且不是所有的数据库都支持truncate table,其中PostgresDialect就不支持
 * //当SaveMode=Append 的时候,则直接写数据就行
 * //当SaveMode=ErrorIfExists 的时候,则直接抛异常
 * //当SaveMode=Ignore 的时候,则直接不做任何事情
 *
 * //按照某个分区字段进行分区读数据
 * //partitionColumn 分区的字段，这个字段必须是integral类型的
 * //lowerBound  用于决定分区步数的partitionColumn的最小值
 * //upperBound  用于决定分区步数的partitionColumn的最大值
 * //numPartitions 分区数，和lowerBound以及upperBound一起来为每一个分区生成sql的where字句
 *
 * //如果upperBound - lowerBound >= numPartitions,那么我们就取numPartitions个分区，
 * // 否则我们取upperBound - lowerBound个分区数
 * // 8 - 3 = 5 > 3 所以我们取3个分区
 * // where id < 3 + 1 这个1是通过 8／3 - 3／3 = 1得来的
 * // where id >= 3 + 1 and id < 3 + 1 + 1
 * // where id >= 3 + 1 + 1
 * //配置的方式
 *
 * //每次读取的时候，可以采用batch的方式读取数据，batch的数量可以由参数fetchsize来设置。默认为：0，表示jdbc的driver来估计这个batch的大小
 *
 * //不管是读还是写，都有分区数的概念，
 * // 读的时候是通过用户设置numPartitions参数设置的，
 * // 而写的分区数是DataFrame的分区数
 * //需要注意一点的是不管是读还是写，每一个分区都会打开一个jdbc的连接，所以分区不宜太多，要不然的话会搞垮数据库
 * //写的时候，可以通过DataFrame的coalease接口来减少分区数
 */
object JdbcDatasourceTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("JdbcDatasourceTest")
      .master("local")
      .getOrCreate()

    //url:
    // jdbc:mysql://master:3306/test
    // jdbc:oracle://master:3306/test
    // jdbc:db2://master:3306/test
    // jdbc:derby://master:3306/test
    // jdbc:sqlserver://master:3306/test
    // jdbc:postgresql://master:3306/test

    val mysqlUrl = "jdbc:mysql://master:3306/test"
    //1: 读取csv文件数据
    val optsMap = Map("header" -> "true","inferSchema" -> "true")
    val df = spark.read.options(optsMap).csv(s"${Utils.BASE_PATH}/jdbc_demo_data.csv")
    df.show()

    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "root")

    //向Mysql数据库写数据
    df.write.mode(SaveMode.Overwrite).jdbc(mysqlUrl,"person",properties)
    //从mysql数据库读取数据
    val jdbcDFWithNoneOption = spark.read.jdbc(mysqlUrl,"person",properties)
    jdbcDFWithNoneOption.show()

    //写数据的过程：
    //1 : 建表
    //第一次写的时候，需要创建一张表，建表语句类似如下：
    //CREATE TABLE t (name string) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1
    //ENGINE=InnoDB使用innodb引擎 DEFAULT CHARSET=utf8 数据库默认编码为utf-8 AUTO_INCREMENT=1 自增键的起始序号为1
    //.InnoDB，是MySQL的数据库引擎之一，为MySQL AB发布binary的标准之一
    //属性配置ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1可以通过参数createTableOptions传给spark

    var writeOpts = Map("createTableOptions" -> "ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1")
    df.write.mode(SaveMode.Overwrite).options(writeOpts).jdbc(mysqlUrl,"person",properties)

    //2: 设置表的schema
    // 一般表的schema是和DataFrame是一致的，字段的类型是从spark sql的DataType翻译到各个数据库对应的数据类型
    // 如果字段在数据库中的类型不是你想要的，
    // 你可以通过参数createTableColumnTypes来设置createTableColumnTypes=age long,name string
    writeOpts = Map("createTableColumnTypes" -> "id long,age long")
    df.write.mode(SaveMode.Overwrite).options(writeOpts).jdbc(mysqlUrl, "person", properties)

    //3: 事务隔离级别的设置，通过参数isolationLevel设置
    //  NONE 不支持事物
    // READ_UNCOMMITTED 会出现脏读、不可重复读以及幻读
    // READ_COMMITTED 不会出现脏读，但是还是会出现不可重复读以及幻读
    // REPEATABLE_READ  不会出现脏读以及不可重复读，但是还会出现幻读
    // SERIALIZABLE   脏读、不可重复读以及幻读都不会出现了
    writeOpts = Map("isolationLevel" -> "READ_UNCOMMITTED")
    df.write.mode(SaveMode.Overwrite).options(writeOpts).jdbc(mysqlUrl, "person", properties)

    //4：写数据
    //写数据的过程中可以采用批量写数据，每一批写的数据量的大小可以通过参数batchsize设置，默认是：1000
    writeOpts = Map("batchsize" -> "3")
    df.write.mode(SaveMode.Overwrite).options(writeOpts).jdbc(mysqlUrl, "person", properties)

    //5：第二次写数据的时候，这个时候表已经存在了，所以需要区分SaveMode
    //当SaveMode=Overwrite 的时候，需要先清理表，然后再写数据。清理表的方法又分两种：
    //  第一种是truncate即清空表，如果是这种的话，则先清空表，然后再写数据
    //  第二种是drop掉表，如果是这种的话，则先drop表，然后建表，最后写数据
    //以上两种方式的选择，可以通过参数truncate(默认是false)控制。因为truncate清空数据可能会失败，所以可以使用drop table的方式
    //而且不是所有的数据库都支持truncate table,其中PostgresDialect就不支持
    //当SaveMode=Append 的时候,则直接写数据就行
    //当SaveMode=ErrorIfExists 的时候,则直接抛异常
    //当SaveMode=Ignore 的时候,则直接不做任何事情
    writeOpts = Map[String, String]("truncate" -> "false")
    df.write.mode(SaveMode.Overwrite).options(writeOpts).jdbc(mysqlUrl, "person", properties)

    //按照某个分区字段进行分区读数据
    //partitionColumn 分区的字段，这个字段必须是integral类型的
    //lowerBound  用于决定分区步数的partitionColumn的最小值
    //upperBound  用于决定分区步数的partitionColumn的最大值
    //numPartitions 分区数，和lowerBound以及upperBound一起来为每一个分区生成sql的where字句

    //如果upperBound - lowerBound >= numPartitions,那么我们就取numPartitions个分区，
    // 否则我们取upperBound - lowerBound个分区数
    // 8 - 3 = 5 > 3 所以我们取3个分区
    // where id < 3 + 1 这个1是通过 8／3 - 3／3 = 1得来的
    // where id >= 3 + 1 and id < 3 + 1 + 1
    // where id >= 3 + 1 + 1
    //配置的方式

    val readOpts = Map[String, String]("numPartitions" -> "3", "partitionColumn" -> "id",
      "lowerBound" -> "3", "upperBound" -> "8", "fetchsize" -> "100")
    val jdbcDF = spark.read.options(readOpts).jdbc(mysqlUrl, "person", properties)
    jdbcDF.rdd.partitions.size
    jdbcDF.rdd.glom().collect()
    jdbcDF.show()

    //api的方式
    spark.read.jdbc(mysqlUrl, "person", "id", 3, 8, 3, properties).show()
    //参数predicates: Array[String],用于决定每一个分区对应的where子句，分区数就是数组predicates的大小
    val conditionDF = spark.read.jdbc(mysqlUrl,
      "person", Array("id > 2 and id < 5", "id >= 5 and id < 8"), properties)
    conditionDF.rdd.partitions.size
    conditionDF.rdd.glom().collect()
    conditionDF.show()

    //每次读取的时候，可以采用batch的方式读取数据，batch的数量可以由参数fetchsize来设置。默认为：0，表示jdbc的driver来估计这个batch的大小

    //不管是读还是写，都有分区数的概念，
    // 读的时候是通过用户设置numPartitions参数设置的，
    // 而写的分区数是DataFrame的分区数
    //需要注意一点的是不管是读还是写，每一个分区都会打开一个jdbc的连接，所以分区不宜太多，要不然的话会搞垮数据库
    //写的时候，可以通过DataFrame的coalease接口来减少分区数
    spark.stop()

  }

}
