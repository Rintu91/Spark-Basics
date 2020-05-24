package com.SparkScala
import org.apache.log4j._
import org.apache.spark.sql._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.functions.window
import java.sql.{Timestamp}


//import org.apache.
object FileStreamingWithWindow {
  case class Node(timestamp: Timestamp, word: String)
  def appendTime(line: String): Array[Node] = {
    val fields = line.split("\\s+")
    val date = new java.util.Date
    val timestamp = new Timestamp(System.currentTimeMillis())
    val wordData: ArrayBuffer[Node] = ArrayBuffer()
    for (i<-0 to fields.length-1){
      val newVal = Node(timestamp,fields(i))
      wordData += newVal
    }
    return wordData.toArray    
  }
  
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
                .builder
                .appName("Streaming Window")
                .master("local[1]")
                .config("spark.sql.warehouse.dir","file:///C:/temp")
                .getOrCreate()
                
    val lines = spark.readStream.text("../inputFile/")
    
    //convert the data to (time,word) format
    import spark.implicits._
    val words = lines.as[String].flatMap(appendTime)
    
    words.printSchema()
    //count words with window sie 10 minutes and slide 5 minutes
    val windowCounts = words.groupBy(
                          window($"timestamp", "10 minutes","5 minutes"),
                          $"word"
                      ).count()
                      
    val query = windowCounts.writeStream
                .outputMode("complete")
                .format("console")
                .start()
                .awaitTermination()
  }
} 