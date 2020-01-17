package com.westar.spark.rdd.scala

import java.io.File

/**
  * Created by tangweiqun on 2017/9/10.
  */
object OptionTest {
  def main(args: Array[String]): Unit = {
    var x: Option[String] = None

    x.get //java.util.NoSuchElementException: None.get

    x.getOrElse("default") // default

    x = Some("Now Initialized")

    x.get // Now Initialized

    x.getOrElse("default") // default

    // Option 伴生对象中提供的工厂方法
    x = Option(null) // x: Option[String] = None

    x = Option("some value") // x: Option[String] = Some(some value)

    // Option 可以被当作集合来看待
    val fileNameOpt: Option[String] = Some("testfile")
    fileNameOpt.map(fileName => {
      println(s"have some value ${fileName}")
      new File(fileName)
    })

    getDir(None)
    getDir(Some("/Users/tangweiqun"))

    val userName = Option("sss")
    for (uname <- userName) {
      println("User: " + uname)
    }
  }

  def getDir(fileNameOpt: Option[String]) : java.io.File = {
    fileNameOpt.map(fileName => new File(fileName))
      .filter(_.isDirectory)
      .getOrElse(new File(System.getProperty("java.io.tmpdir")))
  }
}
