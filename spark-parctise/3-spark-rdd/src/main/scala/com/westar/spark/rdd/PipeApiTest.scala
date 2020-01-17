package com.westar.spark.rdd

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.{SparkConf, SparkContext}

object PipeApiTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("PipeApiTest")
    val sc = new SparkContext(conf)

    val dataRDD = sc.parallelize(List("hi", "hello", "how", "are", "you"), 2)
    //运行进程需要的环境变量
    val env = Map("env" -> "test-env")

    //在执行一个分区task且处理分区输入元素之前将这个分区的全局数据作为脚本的输入的话，则需要定义这个函数
    def printPipeContext(func: String => Unit) = {
      val taskContextData = "this is task context data per partition"
      func(taskContextData)
    }

    //在执行分区task的时候，需要对每一个输入元素做特殊处理的话，可以定义这个函数参数
    def printRDDElement(ele: String, func: String => Unit): Unit = {
      if (ele.equals("hello")) {
        func("dog")
      } else {
        func(ele)
      }
      //表示执行一个本地脚本(可以是shell，python，java等各种能通过java的Process启动起来的脚本进程)
      //dataRDD的数据就是脚本的输入数据，脚本的输出数据会生成一个RDD即pipeRDD
      val pipeRDD = dataRDD.pipe(Seq("python", "/home/hadoop/spark-course/echo.py"),
        env, printPipeContext, printRDDElement, false)

      pipeRDD.glom().collect()

      val pipeRDD2 = dataRDD.pipe("sh /home/hadoop/spark-course/echo.sh")
      pipeRDD2.glom().collect()


      // 你的python脚本所在的hdfs上目录
      // 然后将python目录中的文件代码内容全部拿到
      val scriptsFilesContent = sc.wholeTextFiles("hdfs://hadoop0:9000/users/hadoop-twq/pipe").collect()
      // 将所有的代码内容广播到每一台机器上
      val scriptsFilesB = sc.broadcast(scriptsFilesContent)

      // 创建一个数据源RDD
      val dataRDDTmp = sc.parallelize(List("hi", "hello", "how", "are", "you"), 2)
      // 将广播中的代码内容写到每一台机器上的本地文件中
      dataRDDTmp.foreachPartition( _ => {
        scriptsFilesB.value.foreach { case (filePath, content) => {
          val fileName =filePath.substring(filePath.lastIndexOf("/") + 1)
          val file = new File(s"/home/hadoop/spark-course/pipe/${fileName}")
          if(!file.exists()){
            val buffer = new BufferedWriter(new FileWriter(file))
            buffer.write(content)
            buffer.flush()
            buffer.close()
          }
        }

        }
      })
      // 对数据源rdd调用pipe以分布式的执行我们定义的python脚本
      val hdfsPipeRDD = dataRDDTmp.pipe(s"python /home/hadoop/spark-course/pipe/echo_hdfs.py")
      hdfsPipeRDD.glom().collect()
    }
  }

}
