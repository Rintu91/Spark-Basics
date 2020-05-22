/*
 * Find most popular super hero using number of connections with other super heros
 */
package com.SparkScala
import org.apache.log4j._
import org.apache.spark.{SparkConf,SparkContext}
object SuperHero {
  def countConn(line: String): (Int,Int) = {
    val fields = line.split("\\s+")
    return (fields(0).toInt, (fields.length - 1))
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
  
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[*]").setAppName("Find SuperHero")
    val sc   = new SparkContext(conf)
    //read super hero connected graph and their names data
    val countNumConn = sc.textFile("../../Data/Marvel-graph.txt").map(countConn)
    val superHeroName= sc.textFile("../../Data/Marvel-names.txt").flatMap(getSuperHero)
    //reduce all super hero connections using sper hero id as key
    val getNumConn   = countNumConn.reduceByKey((x,y)=>x+y) 
    //getNumConn.collect.foreach(println)
    //sort the superhero based on number of connections
    val sortedCount  = getNumConn.sortBy(_._2,ascending=false)
    //reverse <K,V> pair to find maximum value
    val maxId        = sortedCount.first()
    //use K to get maximum super hero value
    val mostPopu     = superHeroName.lookup(maxId._1)(0)
    println(s"most popular super hero is: $mostPopu")
    //print top 10 super heros
    val top10 = sortedCount.take(10)
    for(i<-0 to top10.length-1){
       val key = top10(i)._1
       val heroName = superHeroName.lookup(key)(0)
       println(s"$i : Name: $heroName")
    }    
    sc.stop()
  }
}