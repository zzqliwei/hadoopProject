package com.westar.hbase.spark

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
  * 包含一系列的隐式转换，这些隐式转换的功能是使得原生的DStream可以直接调用HBaseContext中与hbase交互的方法
  */
object HBaseDStreamFunctions {

  /**
    * 给DStream丰富和HBase交互的API的隐式类
    *
    * @param dStream  This is for dStreams of any type
    * @tparam T       Type T
    */
  implicit class GenericHBaseDStreamFunctions[T](val dStream: DStream[T]) {

    /**
      * 使得DStream
      * 可以调用HBaseContext中的bulkPut方法
      * 不会返回一个新的RDD，和RDD中的foreach功能类似
      *
      * @param hc         The hbaseContext object to identify which
      *                   HBase cluster connection to use
      * @param tableName  The tableName that the put will be sent to
      * @param f          The function that will turn the DStream values
      *                   into HBase Put objects.
      */
    def hbaseBulkPut(hc: HBaseContext,
                     tableName: TableName,
                     f: (T) => Put): Unit = {
      hc.streamBulkPut(dStream, tableName, f)
    }

    /**
      * Implicit method that gives easy access to HBaseContext's bulk
      * get.  This will return a new DStream.  Think about it as a DStream map
      * function.  In that every DStream value will get a new value out of
      * HBase.  That new value will populate the newly generated DStream.
      *
      * @param hc             The hbaseContext object to identify which
      *                       HBase cluster connection to use
      * @param tableName      The tableName that the put will be sent to
      * @param batchSize      How many gets to execute in a single batch
      * @param f              The function that will turn the RDD values
      *                       in HBase Get objects
      * @param convertResult  The function that will convert a HBase
      *                       Result object into a value that will go
      *                       into the resulting DStream
      * @tparam R             The type of Object that will be coming
      *                       out of the resulting DStream
      * @return               A resulting DStream with type R objects
      */
    def hbaseBulkGet[R: ClassTag](hc: HBaseContext,
                                  tableName: TableName,
                                  batchSize:Int, f: (T) => Get, convertResult: (Result) => R):
    DStream[R] = {
      hc.streamBulkGet[T, R](tableName, batchSize, dStream, f, convertResult)
    }

    /**
      * Implicit method that gives easy access to HBaseContext's bulk
      * get.  This will return a new DStream.  Think about it as a DStream map
      * function.  In that every DStream value will get a new value out of
      * HBase.  That new value will populate the newly generated DStream.
      *
      * @param hc             The hbaseContext object to identify which
      *                       HBase cluster connection to use
      * @param tableName      The tableName that the put will be sent to
      * @param batchSize      How many gets to execute in a single batch
      * @param f              The function that will turn the RDD values
      *                       in HBase Get objects
      * @return               A resulting DStream with type R objects
      */
    def hbaseBulkGet(hc: HBaseContext,
                     tableName: TableName, batchSize:Int,
                     f: (T) => Get): DStream[(ImmutableBytesWritable, Result)] = {
      hc.streamBulkGet[T, (ImmutableBytesWritable, Result)](
        tableName, batchSize, dStream, f,
        result => (new ImmutableBytesWritable(result.getRow), result))
    }

    /**
      * Implicit method that gives easy access to HBaseContext's bulk
      * Delete.  This will not return a new DStream.
      *
      * @param hc         The hbaseContext object to identify which HBase
      *                   cluster connection to use
      * @param tableName  The tableName that the deletes will be sent to
      * @param f          The function that will convert the DStream value into
      *                   a HBase Delete Object
      * @param batchSize  The number of Deletes to be sent in a single batch
      */
    def hbaseBulkDelete(hc: HBaseContext,
                        tableName: TableName,
                        f:(T) => Delete, batchSize:Int): Unit = {
      hc.streamBulkDelete(dStream, tableName, f, batchSize)
    }

    /**
      * Implicit method that gives easy access to HBaseContext's
      * foreachPartition method.  This will ack very much like a normal DStream
      * foreach method but for the fact that you will now have a HBase connection
      * while iterating through the values.
      *
      * @param hc  The hbaseContext object to identify which HBase
      *            cluster connection to use
      * @param f   This function will get an iterator for a Partition of an
      *            DStream along with a connection object to HBase
      */
    def hbaseForeachPartition(hc: HBaseContext,
                              f: (Iterator[T], Connection) => Unit): Unit = {
      hc.streamForeachPartition(dStream, f)
    }

    /**
      * Implicit method that gives easy access to HBaseContext's
      * mapPartitions method.  This will ask very much like a normal DStream
      * map partitions method but for the fact that you will now have a
      * HBase connection while iterating through the values
      *
      * @param hc  The hbaseContext object to identify which HBase
      *            cluster connection to use
      * @param f   This function will get an iterator for a Partition of an
      *            DStream along with a connection object to HBase
      * @tparam R  This is the type of objects that will go into the resulting
      *            DStream
      * @return    A resulting DStream of type R
      */
    def hbaseMapPartitions[R: ClassTag](hc: HBaseContext,
                                        f: (Iterator[T], Connection) => Iterator[R]):
    DStream[R] = {
      hc.streamMapPartitions(dStream, f)
    }
  }
}
