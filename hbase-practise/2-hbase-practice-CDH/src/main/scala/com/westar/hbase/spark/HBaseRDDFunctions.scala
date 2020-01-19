package com.westar.hbase.spark

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * 包含一系列的隐式转换，这些隐式转换的功能是使得原生的RDD可以直接调用HBaseContext中与hbase交互的方法
  */
object HBaseRDDFunctions {

  /**
    * 给RDD丰富和HBase交互的API的隐式类
    *
    * @param rdd This is for rdd of any type
    * @tparam T This is any type
    */
  implicit class GenericHBaseRDDFunctions[T](val rdd: RDD[T]) {

    /**
      * 使得RDD可以调用HBaseContext中的bulkPut方法
      * 不会返回一个新的RDD，和RDD中的foreach功能类似
      *
      * @param hc        The hbaseContext object to identify which
      *                  HBase cluster connection to use
      * @param tableName The tableName that the put will be sent to
      * @param f         The function that will turn the RDD values
      *                  into HBase Put objects.
      */
    def hbaseBulkPut(hc: HBaseContext,
                     tableName: TableName,
                     f: (T) => Put): Unit = {
      hc.bulkPut(rdd, tableName, f)
    }

    /**
      * 使得RDD可以调用HBaseContext中的bulkGet方法
      * 会返回一个新的类型的RDD
      *
      * @param hc            The hbaseContext object to identify which
      *                      HBase cluster connection to use
      * @param tableName     The tableName that the put will be sent to
      * @param batchSize     How many gets to execute in a single batch
      * @param f             这个函数是将客户端的RDD的类型数据转换成Get类型的数据
      * @param convertResult  这个函数的功能是将从hbase查询出来的数据转换成客户端需要的类型数据
      * @tparam R The type of Object that will be coming
      *           out of the resulting RDD
      * @return A resulting RDD with type R objects
      */
    def hbaseBulkGet[R: ClassTag](hc: HBaseContext,
                                  tableName: TableName, batchSize: Int,
                                  f: (T) => Get, convertResult: (Result) => R): RDD[R] = {
      hc.bulkGet[T, R](tableName, batchSize, rdd, f, convertResult)
    }

    /**
      * 使得RDD可以调用HBaseContext中的bulkGet方法
      * 会返回一个新的类型为(ImmutableBytesWritable, Result)的RDD
      *
      * @param hc        The hbaseContext object to identify which
      *                  HBase cluster connection to use
      * @param tableName The tableName that the put will be sent to
      * @param batchSize How many gets to execute in a single batch
      * @param f         The function that will turn the RDD values
      *                  in HBase Get objects
      * @return A resulting RDD with type R objects
      */
    def hbaseBulkGet(hc: HBaseContext,
                     tableName: TableName, batchSize: Int,
                     f: (T) => Get): RDD[(ImmutableBytesWritable, Result)] = {
      hc.bulkGet[T, (ImmutableBytesWritable, Result)](tableName,
        batchSize, rdd, f,
        result => if (result != null && result.getRow != null) {
          (new ImmutableBytesWritable(result.getRow), result)
        } else {
          null
        })
    }

    /**
      * 使得RDD可以调用HBaseContext中的hbaseBulkDelete方法
      *
      * @param hc        The hbaseContext object to identify which HBase
      *                  cluster connection to use
      * @param tableName The tableName that the deletes will be sent to
      * @param f         这个函数是将客户端的RDD的类型数据转换成Delete类型数据
      * @param batchSize The number of Deletes to be sent in a single batch
      */
    def hbaseBulkDelete(hc: HBaseContext,
                        tableName: TableName, f: (T) => Delete, batchSize: Int): Unit = {
      hc.bulkDelete(rdd, tableName, f, batchSize)
    }

    /**
      * 使得RDD可以调用HBaseContext中的foreachPartition方法
      * @param hc The hbaseContext object to identify which HBase
      *           cluster connection to use
      * @param f  This function will get an iterator for a Partition of an
      *           RDD along with a connection object to HBase
      */
    def hbaseForeachPartition(hc: HBaseContext,
                              f: (Iterator[T], Connection) => Unit): Unit = {
      hc.foreachPartition(rdd, f)
    }

    /**
      * 使得RDD可以调用HBaseContext中的mapPartitions方法
      *
      * @param hc The hbaseContext object to identify which HBase
      *           cluster connection to use
      * @param f  This function will get an iterator for a Partition of an
      *           RDD along with a connection object to HBase
      * @tparam R This is the type of objects that will go into the resulting
      *           RDD
      * @return A resulting RDD of type R
      */
    def hbaseMapPartitions[R: ClassTag](hc: HBaseContext,
                                        f: (Iterator[T], Connection) => Iterator[R]):
    RDD[R] = {
      hc.mapPartitions[T, R](rdd, f)
    }
  }

}
