package com.westar.spark.rdd

import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object GraphxTest {

  private val logger = LoggerFactory.getLogger("DatasetTest")

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    if(!conf.contains("spark.master")){
      conf.setMaster("local[*]")
    }
    val spark = SparkSession.builder()
      .appName("GraphxTest")
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext

    val users: RDD[(VertexId, (String, String))] = sc.parallelize(Array((3L, ("小明", "学生")), (7L, ("老王", "博士后")),
      (5L, ("老汤", "教授")), (2L, ("老李", "教授"))))

    val relationships: RDD[Edge[String]] = sc.parallelize(Array(Edge(3L, 7L, "合作者"),
      Edge(5L, 3L, "指导者"),Edge(2L, 5L, "同事"), Edge(5L, 7L, "领导")))

    val defaultUser = ("spark", "默认")

    val graph = Graph(users,relationships,defaultUser)

    // 看看博士后的有多少人
    graph.vertices.filter{case(id,(name, pos)) =>{
      pos == "博士后"
    }}.count

    // 源顶点id大于目标顶点id的数量
    graph.edges.filter{case edge =>{
      edge.srcId > edge.dstId
    }}.count

    val facts = graph.triplets.map(triplet =>{
      triplet.srcAttr._1 + " is the " + triplet.attr +" of " + triplet.dstAttr._1
    })
    facts.collect().foreach(println(_))

  }

}
