package com.SparkScala

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._

object TopMoviesWithDataFrame {
  case class MovieName(movieID: Int, movie: String)
  case class Movie(movieID: Int)  
  def main(args: Array[String]){
    //set log level to ERROR to stop showing unnecessary information
    Logger.getLogger("org").setLevel(Level.ERROR)
    //set spark session
    val spark = SparkSession
    .builder
    .master("local[2]")
    .config("spark.sql.warehouse.dir", "file:////C:/temp") //for windows
    .getOrCreate()
    //read two files as RDDs
    val moviesName = spark.sparkContext.textFile("../../Spark-Basics/Data/ml-100k/u.item",2).map(x=>x.split('|')).map(x=>MovieName(x(0).toInt,x(1)))
    val movieWatchList = spark.sparkContext.textFile("../../Spark-Basics/Data/ml-100k/u.data").map(x=>Movie(x.split('\t')(1).toInt))
    //import implicits for spark sql
    import spark.implicits._
    val moviewDS = movieWatchList.toDF()
    moviewDS.createOrReplaceTempView("moviewDB")
    val countMovies = moviewDS.groupBy("movieID").count().orderBy(desc("count"))
    //convert RDD to DF    
    val moviewNameDS = moviesName.toDF()
    //join two DF
    val result = countMovies.join(moviewNameDS, "movieID").orderBy(desc("count"))
    //take top15 movies
    val top15 = result.take(15)
    //show values
    top15.foreach(println)
    spark.stop()
  }
}
/*
Output:
[50,583,Star Wars (1977)]
[258,509,Contact (1997)]
[100,508,Fargo (1996)]
[181,507,Return of the Jedi (1983)]
[294,485,Liar Liar (1997)]
[286,481,English Patient, The (1996)]
[288,478,Scream (1996)]
[1,452,Toy Story (1995)]
[300,431,Air Force One (1997)]
[121,429,Independence Day (ID4) (1996)]
[174,420,Raiders of the Lost Ark (1981)]
[127,413,Godfather, The (1972)]
[56,394,Pulp Fiction (1994)]
[7,392,Twelve Monkeys (1995)]
[98,390,Silence of the Lambs, The (1991)]
*/
