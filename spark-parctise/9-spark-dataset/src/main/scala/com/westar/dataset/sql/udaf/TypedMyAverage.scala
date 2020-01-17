package com.westar.dataset.sql.udaf

import com.westar.dataset.Person
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

case class Average(var sum: Long, var count: Long)

object TypedMyAverage extends Aggregator[Person, Average, Double]{
  //2: 聚合过程
  //这个聚合函数的zero值
  override def zero: Average = Average(0L, 0L)

  //合并两个值来产生一个新的值
  override def reduce(buffer: Average, person: Person): Average = {
    buffer.sum += person.age
    buffer.count += 1
    buffer
  }

  //合并两个中间值
  override def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count = b2.count
    b1
  }

  //3：函数计算结果
  //得出这个聚合函数的最终的值
  override def finish(reduction: Average): Double = {
    reduction.sum.toDouble / reduction.count
  }

  //1：数据类型定义
  //指定中间值类型的Encoder
  override def bufferEncoder: Encoder[Average] = Encoders.product

  //指定最终结果值类型的Encoder
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
