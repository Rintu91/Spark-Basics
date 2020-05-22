/*Find word count of a text after removing stopwords and some regularization * 
 * 
 */

package com.SparkScala
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.log4j._
object wordCount {
  def main(args: Array[String]){
    val stopWords = Array("a","an","the","in","it","has","are","you","me","i","can","for","on","your","be","as",
                      "have","s","if","with","t","this","will","what","re","about","more","up","there","out","new","just","own",",",
                       "all","by","some","one","than","was","into","much","other","no","yes","day","add","this","those",
                       "to","of","is","but","at","or","they","my","do","not","from","them","work","so","people","","and","we","try",
                       "go","such","its","let","having","come","does","done","doing","here","that","how","get","their")
                       
    def notIn(word: String): Boolean = {      
        if(stopWords.contains(word)){
            return false
        }else{
            return true
        }
    }
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[1]").setAppName("word count")
    //create spark context
    val sc   = new SparkContext(conf)
    //read the text file
    val inputData = sc.textFile("../../Data/book.txt")
    //converts each words to a row in RDD using word regex and converting to upper or lower case and remove stopwords
    val words = inputData.flatMap(_.split("\\W+")).map(_.toLowerCase).filter(x=>notIn(x))
    //map each word as (k,V) pair and reduce by wordname
    val counts= words.map(x=>(x,1)).reduceByKey((x,y)=>x+y)
    //sort each word by their count values
    val sortedCnt = counts.sortBy(_._2, ascending = false)
    //send to driver for console output
    sortedCnt.collect.foreach(println)    
    sc.stop()
  }
}