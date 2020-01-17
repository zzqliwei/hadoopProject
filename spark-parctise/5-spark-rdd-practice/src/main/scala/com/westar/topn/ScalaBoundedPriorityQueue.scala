package com.westar.topn

import java.io.Serializable
import java.util.{PriorityQueue => JPriorityQueue}

import scala.collection.JavaConverters._

class ScalaBoundedPriorityQueue[A](maxSize:Int)(implicit ord:Ordering[A])
 extends Iterable[A] with Serializable {

  private val underlying = new JPriorityQueue[A](maxSize, ord)

  override def size: Int = underlying.size()

  override def iterator: Iterator[A] = underlying.iterator.asScala

  private def maybeReplaceLowest(a: A): Boolean = {
    val head = underlying.peek();
    if(null != head && ord.gt(a,head)){
      underlying.poll()
      underlying.offer(a)
    }else{
      false
    }

  }

  def ++=(xs:Iterable[A]): this.type ={
    xs.foreach(this += _ )
    this
  }

  def +=(elem:A):this.type = {
    if(size < maxSize){
      underlying.offer(elem)
    }else{
      maybeReplaceLowest(elem)
    }
    this
  }

  def +=(elem1:A,elem2:A,elems:A*):this.type ={
    this += elem1 += elem2 ++= elems
  }

  def clear(): Unit ={
    underlying.clear()
  }


}
