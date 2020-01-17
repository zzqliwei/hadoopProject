package com.westar.topn

import com.google.common.collect.{Ordering => GuavaOrdering}

import scala.collection.JavaConverters._

object Utils {
  def takeOrdered[T](input: Iterator[T],num:Int)(implicit ord:Ordering[T]):java.util.List[T] = {
    val ordering = new GuavaOrdering[T]() {
      override def compare(left: T, right: T): Int = ord.compare(left,right)
    };
    ordering.leastOf(input.asJava,num)
  }
}
