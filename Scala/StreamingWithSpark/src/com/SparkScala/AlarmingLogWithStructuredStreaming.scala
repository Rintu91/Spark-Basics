package com.SparkScala
import org.apache.spark.sql._
import org.apache.log4j._
import java.sql.Timestamp
import java.util.regex.Pattern
import java.util.regex.Matcher
import org.apache.spark.sql.functions.window
import java.text.SimpleDateFormat
import java.util.Locale

object AlarmingLogWithStructuredStreaming {
  val datePattern = Pattern.compile("\\[(.*?) .+]")
  //val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
  def apacheLogPattern():Pattern = {
    val ddd = "\\d{1,3}"                      
    val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"  
    val client = "(\\S+)"                     
    val user = "(\\S+)"
    val dateTime = "(\\[.+?\\])"              
    val request = "\"(.*?)\""                 
    val status = "(\\d{3})"
    val bytes = "(\\S+)"                     
    val referer = "\"(.*?)\""
    val agent = "\"(.*?)\""
    val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
    Pattern.compile(regex)    
  }
  
  case class logData(timestamp: String, errorCode: String)
  def getDateTime(value: String): Option[String] ={
    val dateMatcher = datePattern.matcher(value)
      if (dateMatcher.find) {
              val dateString = dateMatcher.group(1)
              val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
              val date = (dateFormat.parse(dateString))
              val timestamp = new java.sql.Timestamp(date.getTime());
              return Option(timestamp.toString())
          } else {
          return None
      }    
  }
  def mapLogData(line: Row): Option[logData] ={
    val pattern = apacheLogPattern()
    val matcher = pattern.matcher(line.getString(0))
    if(matcher.matches()){
      val code = scala.util.Try(matcher.group(6).toInt) getOrElse 0
      val errorCode = {
        if(code>=200 && code<=300){
          "success"
        }else{
          "Failure"
        }
      }
      return Some(logData(
            getDateTime(matcher.group(4)).getOrElse(""), 
            errorCode
            ))      
    }else{
      return None
    }
  }
  def main(args: Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
                .builder
                .appName("Alarming Log")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///C:temp/")
                .getOrCreate()
    val input = spark
                .readStream
                .format("socket")
                .option("host", args(0))
                .option("port",args(1).toInt)
                .load()
    import spark.implicits._
    //convert the data to DataSet to use rdd operations
    val inputDS     = input.flatMap(mapLogData)
    val calFailures = inputDS.groupBy(
                                      window($"timeStamp","5 minutes","1 minute"),
                                       $"errorCode").count().orderBy("window")
    
    val query = calFailures
                .writeStream
                .outputMode("complete")
                .format("console")
                .start()
                .awaitTermination()
                
    spark.stop()
  }
}