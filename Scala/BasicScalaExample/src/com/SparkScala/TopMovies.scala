package com.SparkScala

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

object TopMovies {
  def getMovieNames(): Map[Int,String] = {
    var movieNames: Map[Int,String] = Map()
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    
    val lines = Source.fromFile("../../Spark-Basics/Data/ml-100k/u.item").getLines()
    for(line<-lines){
      val fields = line.split('|')
      if (fields.length >1){
        movieNames += (fields(0).toInt->fields(1))
      }
    }    
    return movieNames
  }
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[2]","top10Movie")
    //read movies data
    val movieID = sc.textFile("../../Spark-Basics/Data/ml-100k/u.data").map(x=>x.split('\t')(1))
    //get the count of entries for each movie ID
    val countViews = movieID.map(x=>(x.toInt,1)).reduceByKey((x,y)=>x+y)
    //countViews.toDebugString
    //sort the the values by desc
    val sortedCountView = countViews.sortBy(_._2, false)
    //take top 10 values
    val top10 = sortedCountView.take(10)
    //read movie name data
    val movieName = getMovieNames()
    //movieName.foreach(println)
    for(i<-top10){
      println(movieName(i._1) + ":" + i._2)
    }    
    sc.stop()
  }
}
