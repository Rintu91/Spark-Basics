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
                                                  //> Using Spark's default log4j profile: org/apache/spark/log4j-defaults.propert
                                                  //| ies
                                                  //| 20/05/16 21:04:28 INFO SparkContext: Running Spark version 3.0.0-preview2
                                                  //| 20/05/16 21:04:28 WARN NativeCodeLoader: Unable to load native-hadoop librar
                                                  //| y for your platform... using builtin-java classes where applicable
                                                  //| 20/05/16 21:04:29 INFO ResourceUtils: ======================================
                                                  //| ========================
                                                  //| 20/05/16 21:04:29 INFO ResourceUtils: Resources for spark.driver:
                                                  //| 
                                                  //| 20/05/16 21:04:29 INFO ResourceUtils: ======================================
                                                  //| ========================
                                                  //| 20/05/16 21:04:29 INFO SparkContext: Submitted application: 89dd8d2c-125a-42
                                                  //| c1-a4fa-42b5ffbd6909
                                                  //| 20/05/16 21:04:29 INFO SecurityManager: Changing view acls to: Jayeeta
                                                  //| 20/05/16 21:04:29 INFO SecurityManager: Changing modify acls to: Jayeeta
                                                  //| 20/05/16 21:04:29 INFO SecurityManager: Changing view acls groups to: 
                                                  //| 20/05/16 21:04:29 INFO SecurityManager:
                                                  //| Output exceeds cutoff limit.
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
  	)).toDF("label","features")               //> 20/05/16 21:04:34 INFO SharedState: Setting hive.metastore.warehouse.dir ('
                                                
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
  //}
  //define the model
  val lr = new LogisticRegression()               //> lr  : org.apache.spark.ml.classification.LogisticRegression = logreg_267e59
                                                  //| 77cf8a
  println(s"LogisticRegression parameters:\n ${lr.explainParams()}\n")
                                                  lr.setMaxIter(2).setRegParam(0.1)                //> res0: com.SparkScala.LogisticRegression.lr.type = logreg_267e5977cf8a

//split the data into (train and val set)
 val trainVal = dataSet.randomSplit(Array(0.8,0.2), 123)
                                                  //> trainVal  : Array[org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]] =
                                                  //|  Array([label: double, features: vector], [label: double, features: vector]
                                                  //| )
 val trainData = trainVal(0)                      //> trainData  : org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [labe
                                                  //| l: double, features: vector]
 val valData   = trainVal(1)                      //> valData  : org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [label:
                                                  //|  double, features: vector]
 
 //fit model on train data
 val model = lr.fit(trainData)                    //> 20/05/16 21:04:38 INFO CodeGenerator: Code generated in 89.4796 ms
                                                  
 println(s"Model 1 was fit using parameters: ${model.parent.extractParamMap}")
 //check the parameters of the model
 println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

val predictions = model.transform(valData)
//println(predictions)
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