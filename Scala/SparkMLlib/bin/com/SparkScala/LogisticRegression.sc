package com.SparkScala

object LogisticRegression {
  println("Welcome to the Scala worksheet")
  
  //def main(args: Array[String]){
  	//set log level
  	//Logger.getLogger("org").setLevel(Level.ERROR)
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
  //}
  //define the model
  val lr = new LogisticRegression()
  println(s"LogisticRegression parameters:\n ${lr.explainParams()}\n")
 lr.setMaxIter(2).setRegParam(0.1)

//split the data into (train and val set)
 val trainVal = dataSet.randomSplit(Array(0.8,0.2), 123)
 val trainData = trainVal(0)
 val valData   = trainVal(1)
 
 //fit model on train data
 val model = lr.fit(trainData)
 
 println(s"Model 1 was fit using parameters: ${model.parent.extractParamMap}")
 
val cmd = "my_lr_model" +: model.intercept.toString +: model.coefficients.toArray.mkString(",").split(",")
println(cmd)

val predictions = model.transform(valData)
println(predictions)

 
  
  
}