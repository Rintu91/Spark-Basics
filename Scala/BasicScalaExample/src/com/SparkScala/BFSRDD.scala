/*
 * BFS Traversal with basic RDD operations
 * use super hero data and find distance of each super hero from captain america
 */

package com.SparkScala
import org.apache.log4j._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._
object BFSRDD {
  type Data = (Array[Int],Int)
  type Node = (Int,Data)
  val sourceId= 859
  
  def readGraph(lines: String) : Node ={
    val fields = lines.split("\\s+")
    val src    = fields(0).toInt
    var dstList: ArrayBuffer[Int]= ArrayBuffer()
    for(dst<-1 to (fields.length-1)){
      dstList += fields(dst).toInt      
    }
    var distance = 9999
    if(src==sourceId)
      distance = 0
    return (src,(dstList.toArray,distance))
  }
  def getSuperHero(line: String): Option[(Int,String)] ={
    val fields = line.split('\"')
    if (fields.length > 1){
      return Some(fields(0).trim().toInt, fields(1))
    }
    else{
      return None
    }
  }
  def reduceList(val1: Data, val2: Data): Data = {
    val list1: Array[Int] = val1._1
    val dist1 = val1._2
    val list2: Array[Int] = val2._1
    val dist2 = val2._2
    
    var appendedList: ArrayBuffer[Int] = ArrayBuffer()
    appendedList ++= list1
    appendedList ++= list2
    val dist = math.min(dist1,dist2)
    return (appendedList.toArray, dist)
  }
  def exploreList(value: Node, stepId: Int): Array[Node] ={
    val srcId = value._1
    //get the distance of current vertex
    val dist  = value._2._2
    
    val edgeBuffer: ArrayBuffer[Node] = ArrayBuffer()
    //if the stepId matches then explore adjacent and update their distance as (dist+1)
    if(dist==stepId){
      val dstList = value._2._1
      for(j<-0 to (dstList.length-1)){
        val dst = dstList(j)
        val nEdge: Node = (dst,(Array(),dist+1))
        edgeBuffer += nEdge        
      }
    }
    edgeBuffer += value
    return edgeBuffer.toArray
  }
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("BFS Super Hero").setMaster("local[2]")
    val sc   = new SparkContext(conf)
    val graph= sc.textFile("../../Data/Marvel-graph.txt").map(readGraph)
    val names= sc.textFile("../../Data/Marvel-names.txt").flatMap(getSuperHero)
    var bfsAns= graph.reduceByKey(reduceList)  
    
    for(s<-0 to 10){   
      //for each adjacent vertex create an row in RDD
     val mapRDD = bfsAns.flatMap(x=>exploreList(x, s))
     //if(accum.value == 0)
     //  loop.break
     //reduce the RDD and create a single row for each vertex
     bfsAns = mapRDD.reduceByKey(reduceList)
    // s += 1     
    }
    //get distance of each super hero from spider man
    val eachDist = bfsAns.map(x=>(x._1,x._2._2))
    val ansRDD   = eachDist.join(names)
    val sortedAns= ansRDD.sortBy(_._2._1)
    //sortedAns.collect.foreach(x=>println(x._2._1, x._2._2))
    //show nearest 10
    val near10 = sortedAns.take(10)
    for(i<-0 to near10.length-1){
      println(near10(i)._2._1+" "+near10(i)._2._2)
    }
  }
}