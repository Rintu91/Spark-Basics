package com.SparkScala

object LogisticRegression {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(95); 
  println("Welcome to the Scala worksheet");$skip(230); 
  
  //def main(args: Array[String]){
  	//set log level
  	//Logger.getLogger("org").setLevel(Level.ERROR)
  	val spark = SparkSession.builder.master("local[2]").config("spark.sql.warehouse.dir","file:////C:/temp").getOrCreate();System.out.println("""spark  : <error> = """ + $show(spark ));$skip(491); 
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
  	)).toDF("label","features");System.out.println("""dataSet  : <error> = """ + $show(dataSet ));$skip(19); val res$0 = 
    dataSet.show();System.out.println("""res0: <error> = """ + $show(res$0));$skip(63); 
  //}
  //define the model
  val lr = new LogisticRegression();System.out.println("""lr  : <error> = """ + $show(lr ));$skip(71); 
  println(s"LogisticRegression parameters:\n ${lr.explainParams()}\n");$skip(35); val res$1 = 
 lr.setMaxIter(2).setRegParam(0.1);System.out.println("""res1: <error> = """ + $show(res$1));$skip(100); 

//split the data into (train and val set)
 val trainVal = dataSet.randomSplit(Array(0.8,0.2), 123);System.out.println("""trainVal  : <error> = """ + $show(trainVal ));$skip(29); 
 val trainData = trainVal(0);System.out.println("""trainData  : <error> = """ + $show(trainData ));$skip(29); 
 val valData   = trainVal(1);System.out.println("""valData  : <error> = """ + $show(valData ));$skip(60); 
 
 //fit model on train data
 val model = lr.fit(trainData);System.out.println("""model  : <error> = """ + $show(model ));$skip(81); 
 
 println(s"Model 1 was fit using parameters: ${model.parent.extractParamMap}");$skip(109); 
 
val cmd = "my_lr_model" +: model.intercept.toString +: model.coefficients.toArray.mkString(",").split(",");System.out.println("""cmd  : <error> = """ + $show(cmd ));$skip(13); 
println(cmd);$skip(44); 

val predictions = model.transform(valData);System.out.println("""predictions  : <error> = """ + $show(predictions ));$skip(21); 
println(predictions)}

 
  
  
}
