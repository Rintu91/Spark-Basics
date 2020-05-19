package com.SparkScala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object ShortestPath {
  //one way to read edgelist format
  //assign random weights to edges
  def readData(lines: String):Edge[Double] = {
    val rnd  = scala.util.Random
    val fields = lines.split(" ")
    val edge = Edge(fields(0).toLong, fields(1).toLong, rnd.nextInt(100).toDouble)
    edge
  }
def main(args: Array[String]){
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setMaster("local[2]").setAppName("SSSP")
  val sc   = new SparkContext(conf)
  val graphEdges = sc.textFile("../../Spark-Basics/Data/samplePageRank.txt").map(readData)
  val G = Graph.fromEdges(graphEdges, 0)
  //print the input graph
  G.edges.collect.foreach(println)
  
  //define source verted
  val sourceId = 0
  //initialize the source as 0 and others as infinity
  val initialGraph = G.mapVertices((id,_)=> if(id==sourceId) 0.0 else Double.PositiveInfinity)
  val sssp = initialGraph.pregel(Double.PositiveInfinity, 4)(      
      //vprog 
      (id,attr,msg)=>math.min(attr,msg),
      //sendMsg 
      triplet=>{
        if(triplet.srcAttr!=Double.PositiveInfinity && triplet.srcAttr+triplet.attr < triplet.dstAttr){
         // if (triplet.srcAttr+triplet.attr < triplet.dstAttr){
            Iterator((triplet.dstId,triplet.srcAttr+triplet.attr))
          //}
        }else{
          Iterator.empty
        }
      },
      //mergeMsg
      (a,b)=>math.min(a,b)      
      )
  println("SSSP Result")
  sssp.vertices.collect.foreach(println)
  
  sc.stop()
  }
}