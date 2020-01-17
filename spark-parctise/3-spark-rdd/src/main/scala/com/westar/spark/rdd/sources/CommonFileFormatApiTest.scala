package com.westar.spark.rdd.sources

import java.io.{StringReader, StringWriter}

import au.com.bytecode.opencsv.{CSVReader, CSVWriter}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
 * textFile
 * saveAsSequenceFile
 * saveAsObjectFile
 * objectFile
 */
object CommonFileFormatApiTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test")

    val sc = new SparkContext(conf)
    // text file format
    val data = sc.parallelize(Seq("i am the first test", "what about you", "hello world"), 3)

    data.saveAsTextFile("hdfs://hadoop:9000/users/hadoop/text/")

    val textFileInputFromHdfs = sc.textFile("hdfs://hadoop:9000/users/hadoop/text/part-00001")
    textFileInputFromHdfs.collect()

    // csv file format
    val persons = sc.parallelize(Seq(Person("jeffy", 30), Person("tom", 24)), 1)
    persons.map(person => List(person.name, person.age.toString).toArray).mapPartitions(people => {
      import scala.collection.JavaConversions._
      val stringWriter = new StringWriter()
      val csvWriter = new CSVWriter(stringWriter)
      csvWriter.writeAll(people.toList)
      Iterator(stringWriter.toString)
    }).saveAsTextFile("hdfs://hadoop0:9000/users/hadoop/csv/")

    val peopleWithCsv = sc.textFile("hdfs://hadoop0:9000/users/hadoop/csv/part-00000").map(line => {
      val reader = new CSVReader(new StringReader(line))
      reader.readNext()
    })
    peopleWithCsv.collect()

    // sequence file format
    val sequenceFileData = sc.parallelize(List(("panda", 3), ("kay", 6), ("snail", 2)))

    sc.hadoopConfiguration.setBoolean(FileOutputFormat.COMPRESS, true)
    sc.hadoopConfiguration.set(FileOutputFormat.COMPRESS_TYPE, CompressionType.NONE.toString)
    //sc.hadoopConfiguration.set(FileOutputFormat.COMPRESS_TYPE, CompressionType.RECORD.toString)
    //sc.hadoopConfiguration.set(FileOutputFormat.COMPRESS_TYPE, CompressionType.BLOCK.toString)
    sequenceFileData.saveAsSequenceFile("hdfs://hadoop0:9000/users/hadoop/sequence/")

    val sequenceFileInput = sc.sequenceFile("hdfs://hadoop0:9000/users/hadoop/sequence/part-00003",
      classOf[Text], classOf[IntWritable])

    //sequenceFileInput.collect()

    sequenceFileInput.map { case (x, y) => (x.toString, y.get()) }.collect()

    // object file format
    // 就是key为org.apache.hadoop.io.NullWritable的sequence file
    persons.saveAsObjectFile("hdfs://master:9999/users/hadoop-twq/object")

    val objectFileInput = sc.objectFile[Person]("hdfs://master:9999/users/hadoop-twq/object/part-00000")
    objectFileInput.collect()

  }

}
case class Person(name: String, age: Int)
