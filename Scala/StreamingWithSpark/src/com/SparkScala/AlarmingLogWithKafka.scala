package com.SparkScala
/*check real access_log from an web-site
 * if more than 50% status code is error for 5minutes then raise an alarm
 * include below jars:
 * spark-streaming-kafka-0–10_2.12–3.0.0-preview
	 kafka-clients-2.4.1
   streaming-kafka_2.12–0.8.0
   spark-token-provider-kafka-0–10_2.12
   include all jars from kafka
   
   --procedure to run
   1. Run zkserver
   2. Run kafka-server
   3. create a topic name apacheLog: kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic apacheLog
   4. Run the program
   5. Run producer with access_log: kafka-console-producer.bat --broker-list localhost:9092 --topic apacheLog < access_log
 */
import org.apache.spark.streaming.StreamingContext
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.log4j._
import org.apache.spark.streaming.Seconds
import java.util.regex.Pattern
import java.util.regex.Matcher
import org.apache.spark.streaming.kafka010._
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
object AlarmingLogWithKafka {
  def giveMeKafkaProps(params:Array[String]) : Map[String, Object] ={    
     val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> params(0),
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> params(1),
      )
      return kafkaParams
  }
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
    val conf = new SparkConf().setAppName("Alarming Log Kafka").setMaster("local[*]")
   
    val ssc = new StreamingContext(conf, Seconds(1)) //batch interval 1 seconds
    
    //val kafkaParams = Map("metadata.broker.list"->"localhost:9092")
    val kafkaParams = giveMeKafkaProps(Array("localhost:9092","console-consumer-23162"))
    val topic       = List("apacheLog").toSet
    val input = KafkaUtils.createDirectStream[String,String](ssc, LocationStrategies.PreferConsistent,
                                                             ConsumerStrategies.Subscribe[String, String](topic, kafkaParams))
                                                             .map(x=>x.value)
    
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