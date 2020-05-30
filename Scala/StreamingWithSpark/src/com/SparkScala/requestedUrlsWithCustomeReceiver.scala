package com.SparkScala
/*
 * Define a custom receiver class for streaming data
 * After receiving count the maximum number of request urls 
 */
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.log4j._
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import java.util.regex.Pattern
import java.util.regex.Matcher

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets
class CustomReceiver(host: String, port: Int) 
        extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2){
  def onStart(){
    new Thread("socket receiver")
    {
      override def run(){
        receive()
      }
    }.start()
  }
  
  def onStop(){
    //do nothing
  }
  
  private def receive(){
    var socket: Socket = null
    var userInput: String = null
    try{
      socket = new Socket(host,port)
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, "UTF-8"))
      userInput = reader.readLine()
      while(!isStopped && userInput!= null){
        store(userInput)
        userInput = reader.readLine()
      }
      reader.close()
      socket.close()
    }catch{
      case e: java.net.ConnectException =>
          restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        restart("Error receiving data", t)      
    }
  }
}
object requestedUrlsWithCustomeReceiver {
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
    val conf = new SparkConf().setAppName("Custom Receiver").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(1))
    
    val host = args(0)
    val port = args(1).toInt
    val input = ssc.receiverStream(new CustomReceiver(host, port)) 
    val pattern = apacheLogPattern()
    val requestUrl = input.map(x=>
                          {
                            val matcher = pattern.matcher(x)
                            if(matcher.matches()){
                              (matcher.group(5).split(" ")(1),1)
                            }else{
                              ("[error]",1)
                            }
                          })
    val aggRequestUrl = requestUrl.reduceByKeyAndWindow(_+_, _-_, Seconds(300), Seconds(1))
    val sortReqUrl    = aggRequestUrl.transform(rdd=>rdd.sortBy(_._2, false))
    sortReqUrl.print()  
    
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}