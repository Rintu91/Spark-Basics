package com.SparkScala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j._
object CreateGraph {
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[2]").setAppName("Create Graph")
    val sc = new SparkContext(conf)
    //create vertex RDD
    val V: RDD[(VertexId,String)] = sc.parallelize(Seq(
                                       (1,"cluster1"),
                                       (2,"cluster2"),
                                       (3,"student"),
                                       (4,"professor"),
                                       (5,"system Admin")
                                      ))
    //create edge RDD
    val E: RDD[Edge[String]]      = sc.parallelize(Seq(
                                       Edge(3L,1L,"uses"),
                                       Edge(4L,1L,"owns"),
                                       Edge(5L,1L,"manages"),
                                       Edge(5L,2L,"manages"),
                                       Edge(3L,4L,"student of"),
                                       Edge(5L,4L,"employee of")
                                     ))
    //create graph
    val G = Graph(V, E)
    //the Graph RDD provides an vertices view
    G.vertices.collect.foreach(println)
    //the graph RDD provides and edges view
    G.edges.collect.foreach(println)
    //the graph RDD provides an triplet view which provides source and destination attr with edge attr
    G.triplets.collect.foreach(println)
    //compute degree of each vertex using GraphOps
    val deg = G.inDegrees
    deg.collect.foreach(println)
  }
}