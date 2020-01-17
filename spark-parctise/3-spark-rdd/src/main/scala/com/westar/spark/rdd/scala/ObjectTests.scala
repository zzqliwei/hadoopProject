package com.westar.spark.rdd.scala

trait 动物 {

  def 吃()

}

trait 可以跑的 {

  def 跑() = {
    println("很正常的跑")
  }

}

class 小狗 extends 动物 with 可以跑的 {

  override def 吃(): Unit = {

    小狗.叫喊()

    println(s"小狗狗[${小狗.小狗的默认名字}]吃东西很优雅")

    小狗.看家()

  }

}

//小狗的伴生对象
object 小狗 {
  private val 小狗的默认名字 = "嘟嘟"

  private def 叫喊() = {
    println("汪汪的叫")
  }

  def 看家(): Unit = {
    println("吃完后，我就会看家，所以我很酷")
  }
}

class 猫猫(名字: String) extends 动物 with 可以跑的 {

  def this() = this("小可爱")

  override def 吃(): Unit = {

    println(s"猫咪[${名字}]吃东西很腼腆")

    小狗.看家()


  }

  override def 跑(): Unit = {
    println("我是用猫步来跑的。。。哈哈哈哈")

  }

}


object 入口 {

  def main(args: Array[String]): Unit = {
    val 小狗 = new 小狗()
    //val修饰的变量是不可改变的
    val 可爱的猫咪 = new 猫猫()
    //var修饰的变量是可以改变的
    var 笨笨的猫咪 = new 猫猫("小笨蛋")

    喂食(小狗)
    喂食(可爱的猫咪)
    喂食(笨笨的猫咪)

    跑步(小狗)
    跑步(可爱的猫咪)
    跑步(笨笨的猫咪)

    //可爱的猫咪 = new 猫猫()

    笨笨的猫咪 = new 猫猫("小乖乖")

  }

  def 喂食(动物: 动物): Unit = {
    动物.吃()
  }

  def 跑步(跑步者: 可以跑的): Unit = {
    跑步者.跑()
  }
}