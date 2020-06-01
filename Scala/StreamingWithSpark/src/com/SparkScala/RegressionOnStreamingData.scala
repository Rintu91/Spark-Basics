package com.SparkScala
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.log4j._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
object RegressionOnStreamingData {
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("Streaming Regression").setMaster("local[*]")
    val ssc  = new StreamingContext(conf, Seconds(1))
    val trainDataInput = ssc.socketTextStream("localhost", 7779)
    val trainData = trainDataInput.map(LabeledPoint.parse)
    trainData.print()
    val numFeatures = 1
    val model = new StreamingLinearRegressionWithSGD().setInitialWeights(Vectors.zeros(numFeatures))
    model.algorithm.setIntercept(true)
    model.trainOn(trainData)
    
    val testInputData = ssc.socketTextStream("localhost", 7780)
    val testData = testInputData.map(LabeledPoint.parse)
    model.predictOnValues(testData.map(data=>(data.label, data.features))).print()
    
    ssc.start()
    ssc.awaitTermination()
  }
}