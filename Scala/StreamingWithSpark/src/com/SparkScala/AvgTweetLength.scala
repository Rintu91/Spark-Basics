package com.SparkScala
import org.apache.log4j._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.twitter._
import java.util.concurrent._
import java.util.concurrent.atomic._
object AvgTweetLength {
  def setupTwitter(fileName: String) = {
    import scala.io.Source
    
    for (line <- Source.fromFile(fileName).getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf   = new SparkConf().setMaster("local[2]").setAppName("Avg Tweet Length")
    val ssc    = new StreamingContext(conf, Seconds(1))
    
    setupTwitter(args(0))
    
    val lines   = TwitterUtils.createStream(ssc, None)
    
    val tweets  = lines.map(x=>x.getText())
    val tweetLen= tweets.map(x=>x.length())
    
    val numTweets= new AtomicLong(0)
    val lenTweets= new AtomicLong(0)
    val maxLenTweet = new AtomicLong(0)
    val minLenTweet = new AtomicLong(9999)
    
    tweetLen.foreachRDD((rdd,time) =>
      {
        val countTweets = rdd.count()
        if(countTweets>0){
          numTweets.getAndAdd(countTweets)
          lenTweets.getAndAdd(rdd.reduce((x,y)=>x+y))
          maxLenTweet.accumulateAndGet(rdd.max(), math.max)
          minLenTweet.accumulateAndGet(rdd.min(), math.min)
          println(s"avg Tweet length: ${lenTweets.get()/numTweets.get()} and max lenth: $maxLenTweet and min length: $minLenTweet")          
        }      
    })
    
    ssc.start()
    ssc.awaitTermination()
  }
}