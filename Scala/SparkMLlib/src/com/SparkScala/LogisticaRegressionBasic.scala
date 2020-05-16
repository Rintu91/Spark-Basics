package com.SparkScala
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.ml.linalg.{Vector,Vectors}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object LogisticaRegressionBasic {
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.master("local[2]").config("spark.sql.warehouse.dir","file:////C:/temp").getOrCreate()                                                  
		val dataSet = spark.createDataFrame(Seq(
		(1.0, Vectors.dense(2.4,1.2,4.5,5.7,6.8)),
		(1.0, Vectors.dense(1.4,1.2,2.3,5.7,4.7)),
		(0.0, Vectors.dense(2.4,-2.2,-1.5,3.7,6.8)),
		(1.0, Vectors.dense(2.4,-1.2,4.5,-5.7,6.8)),
		(0.0, Vectors.dense(-3.2,1.2,4.5,-3.7,6.8)),
		(1.0, Vectors.dense(1.9,0.3,-0.5,1.7,1.8)),
		(0.0, Vectors.dense(4.4,0.2,2.5,1.7,0.9)),
		(0.0, Vectors.dense(-2.4,0.2,0.5,8.7,2.8)),
		(1.0, Vectors.dense(-0.4,0.2,3.2,1.7,9.8))
		)).toDF("label","features")               
                                                
    dataSet.show()                                                                              
   //| +-----+--------------------+
   //| |label|            features|
   //| +-----+--------------------+
   //| |  1.0|[2.4,1.2,4.5,5.7,...|
   //| |  1.0|[1.4,1.2,2.3,5.7,...|
   //| |  0.0|[2.4,-2.2,-1.5,3....|
   //| |  1.0|[2.4,-1.2,4.5,-5....|
   //| |  0.0|[-3.2,1.2,4.5,-3....|
   //| |  1.0|[1.9,0.3,-0.5,1.7...|
   //| |  0.0|[4.4,0.2,2.5,1.7,...|
   //| |  0.0|[-2.4,0.2,0.5,8.7...|
   //| |  1.0|[-0.4,0.2,3.2,1.7...|
   //| +-----+--------------------+
   //| 
 
   //define the model
   val lr = new LogisticRegression()                                                            
   println(s"LogisticRegression parameters:\n ${lr.explainParams()}\n")
                                                  

  //split the data into (train and val set)
  val trainVal = dataSet.randomSplit(Array(0.8,0.2), 123)                                                                                            
  val trainData = trainVal(0)                                                                       
  val valData   = trainVal(1)                      
                                                  
 
  //fit model on train data
  val model = lr.fit(trainData)                    
                                                  
  //check the parameters of the model
  println(s"Model 1 was fit using parameters: ${model.parent.extractParamMap}")
  println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

  val predictions = model.transform(valData)
  predictions.show()
 /*
	 * +-----+--------------------+--------------------+--------------------+----------+
	|label|            features|       rawPrediction|         probability|prediction|
	+-----+--------------------+--------------------+--------------------+----------+
	|  1.0|[2.4,-1.2,4.5,-5....|[-0.3966954468258...|[0.40210655258232...|       1.0|
	|  0.0|[4.4,0.2,2.5,1.7,...|[-2.9932395160071...|[0.04773222541721...|       1.0|
	+-----+--------------------+--------------------+--------------------+----------+
 */
  //evaluate the model
  val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy")
  val accuracy = evaluator.evaluate(predictions)
  //println(accuracy)
  println(s"Test Error = ${(1.0 - accuracy)}")
  spark.stop()
  }
}