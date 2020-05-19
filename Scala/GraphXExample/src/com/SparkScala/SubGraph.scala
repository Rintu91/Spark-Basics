package com.SparkScala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j._
import org.apache.spark.graphx.GraphLoader
object SubGraph {
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[2]").setAppName("Create Graph")
    val sc = new SparkContext(conf)
    
    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
    sc.parallelize(Seq((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
                       (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
                       (4L, ("peter", "student"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
    sc.parallelize(Seq(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
                       Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
                       Edge(4L, 0L, "student"),   Edge(5L, 0L, "colleague")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)
    
    // Notice that there is a user 0 (for which we have no information) connected to users
    // 4 (peter) and 5 (franklin).
    graph.triplets.collect.foreach(println)
    // Remove missing vertices as well as the edges to connected to them
    val validGraph = graph.subgraph(epred = triplet => triplet.dstAttr._2 != "Missing")
    // The valid subgraph will disconnect users 4 and 5 by removing user 0
    validGraph.vertices.collect.foreach(println)
    validGraph.triplets.collect.foreach(println)
    
    val ccGraph = validGraph.connectedComponents() 
    ccGraph.triplets.collect.foreach(println)
    
    val rGraph = GraphLoader.edgeListFile(sc, "../../Spark/data/graphx/followers.txt")
    rGraph.vertices.foreach(println)
    println("CC Result:")
    val ccGraph1 = rGraph.connectedComponents()
    ccGraph1.vertices.foreach(println)
    println("Triangle Counting Result")
    val tcCount = rGraph.triangleCount()
    tcCount.vertices.foreach(println)
    println("PageRank Result")
    val pRank = rGraph.pageRank(0.001)
    pRank.vertices.foreach(println)
  }
}