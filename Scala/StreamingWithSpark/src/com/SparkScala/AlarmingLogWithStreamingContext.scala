package com.SparkScala
/*check real access_log from an web-site
 * if more than 50% status code is error for 5minutes then raise an alarm
 */
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.log4j._
import org.apache.spark.streaming.Seconds
import java.util.regex.Pattern
import java.util.regex.Matcher

object AlarmingLogWithStreamingContext {
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
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("Alarming Log").setMaster("local[*]")
   //get ip and port as arguments 
    if(args.length!=2){
      println("Please Provide ip and port id")
      System.exit(0)
    }
    val ssc = new StreamingContext(conf, Seconds(1)) //batch interval 1 seconds
    val input = ssc.socketTextStream(args(0),args(1).toInt)
    
    //check log pattern
    val pattern = apacheLogPattern()
    val logStatus= input.map(x=>{
      val matcher = pattern.matcher(x)
      if(matcher.matches()){
        matcher.group(6)
      }else{
        "[error]"
      }
    })
    val logCodes = logStatus.map(x=>{
      val code= util.Try(x.toInt) getOrElse 0
      if(code>=200 && code<300){
        ("Success")
      }else{
        ("Failure")
      }
    })
    
    var successCnt: Long = 0
    var FailureCnt: Long = 0
    val windowedSatus = logCodes.countByValueAndWindow(Seconds(300), Seconds(300))
    //logStatus.print()
    windowedSatus.foreachRDD((rdd,time)=>
      {
       val counts = rdd.collect()
       for(i<-counts){
         val res = i._1
         val cnt = i._2
         if(res=="Success"){
           successCnt += cnt
         }else{
           FailureCnt += cnt
         }         
       }     
       
       println(s"Success Count: $successCnt and Error count: $FailureCnt")
       if(FailureCnt>100){
         val ratioError = util.Try(FailureCnt.toDouble/successCnt.toDouble) getOrElse 1.0
         if(ratioError>0.5){
           println("Wake up someone! something is wrong!")
         }
       }
      })
    
    ssc.checkpoint("./checkpoint/")
    ssc.start()
    ssc.awaitTermination()    
  }
}