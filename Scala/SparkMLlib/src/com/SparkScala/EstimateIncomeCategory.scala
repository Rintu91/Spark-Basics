package com.SparkScala

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
object EstimateIncomeCategory {
  case class AdultData(
    age: Double,
    workclass: String,
    fnlwgt: Double,
    education: String,
    education_num: Double,
    marital_status: String,
    occupation: String,
    relationship: String,
    race: String,
    sex: String,
    capital_gain: Double,
    capital_loss: Double,
    hours_per_week: Double,
    native_country: String,
    income: String    
  )
  def readCsv(lines: String): AdultData={
    val fields = lines.split(',')
    val fData  = AdultData(fields(0).toDouble, fields(1), fields(2).toDouble, fields(3), fields(4).toDouble, fields(5),
                            fields(6),fields(7),fields(8),fields(9),fields(10).toDouble,fields(11).toDouble,fields(12).toDouble,
                            fields(13),fields(14))
    fData   
  }
  
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.master("local[2]").appName("Estimate Income")
    .config("spark.sql.warehouse.dir", "file:////C:/temp").getOrCreate()
    import spark.implicits._
    //read the data as DF
    val adultData = spark.sparkContext.textFile("../../Spark-Basics/Data/adult.data").map(readCsv).toDF()   
    //show data
    adultData.show()
    //categorical and numerical data need to be handled separately
    val labelCol = "income"
    val inputCategoricalCols  = Array("workclass","education","marital_status","occupation","relationship","race","sex","native_country")
    //var outputCategoricalCols: Array[String] = new Array[String](inputCategoricalCols.length)
    //for(i<- 0 until inputCategoricalCols.length){
    //  outputCategoricalCols(i) = inputCategoricalCols(i)+"_out";
   // }
    val outputCategoricalCols: Array[String] = inputCategoricalCols.map(x=>(x+"_out"))
    val oneHotCategoricalCols: Array[String] = inputCategoricalCols.map(x=>(x+"_one"))
    //outputCategoricalCols.foreach(println)
    val numericalCols = Array("age","fnlwgt","education_num","capital_gain","capital_loss","hours_per_week")
    val featureCols   = numericalCols ++ oneHotCategoricalCols
    
    //encode labelCol
    val labelIndexer = new StringIndexer().setInputCol(labelCol).setOutputCol("label")    
    //encode categoricalCols
    val categoricalIndexer = new StringIndexer().setInputCols(inputCategoricalCols).setOutputCols(outputCategoricalCols)
    //outputCategoricalCols.foreach(println)
    val enocodedCategorical= new OneHotEncoder().setInputCols(outputCategoricalCols).setOutputCols(oneHotCategoricalCols)
    //assemble numericalCols and one-hot encoded features
    val assembledFeatures  = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    
    //transform the input data
    val pipeStages = Array(labelIndexer,categoricalIndexer)
    val pipe = new Pipeline().setStages(pipeStages)
    val processedDataModel = pipe.fit(adultData)
    val processedAdultData = processedDataModel.transform(adultData)
    
    val pipeStages1 = Array(enocodedCategorical,assembledFeatures)
    val pipe1 = new Pipeline().setStages(pipeStages1)
    val processedDataModel1 = pipe1.fit(processedAdultData)
    val processedAdultData1 = processedDataModel1.transform(processedAdultData)
    
    val dataSet = processedAdultData1.select("label","features")
    dataSet.show()  
    
    //split the data for validation
    val trainVal  = dataSet.randomSplit(Array(0.7,0.3), 46)
    val trainData = trainVal(0)
    val valData   = trainVal(1)
    
    //fit model on train data
    val lr = new LogisticRegression()
    //val model = lr.fit(trainData)  
    
    
    //use parameter tuning using PramaGridBuilder
    val paramBuilder = new ParamGridBuilder()
                        .addGrid(lr.regParam, Array(0.01,0.5,2.0))
                        .addGrid(lr.maxIter,Array(5,10,20))
                        .addGrid(lr.elasticNetParam,Array(0.2,0.4,0.8))
                        .build()
                        
   //create 5 fold cross validator
    val crossValidator = new CrossValidator()
                         .setEstimator(lr)
                         .setEvaluator(new BinaryClassificationEvaluator)
                         .setEstimatorParamMaps(paramBuilder)
                         .setNumFolds(5)
                         .setParallelism(2)
                         
    //it returns the best model
    val cvModel = crossValidator.fit(trainData)
    val lrBestModel = cvModel.bestModel
    println(s"Model was fit using parameters: ${lrBestModel.parent.extractParamMap}")
    //check performance on test data
    val predictions = lrBestModel.transform(valData)
    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label")
    println("ROC using Logistic Regression: "+evaluator.evaluate(predictions))  
    
   
    //use GBT and check
    val gbt = new GBTClassifier()
    val gbtParam = new ParamGridBuilder()
                  .addGrid(gbt.maxDepth,Array(2,3,4,5))
                  .addGrid(gbt.maxIter,Array(10,15,20))
                  .build()
    val gbtCV    = new CrossValidator()
                    .setEstimator(gbt)
                    .setEvaluator(new BinaryClassificationEvaluator)
                    .setEstimatorParamMaps(gbtParam)
                    .setNumFolds(5)
                    .setParallelism(2)
                    
    val gbtModel = gbtCV.fit(trainData)
    val gbtBestModel = gbtModel.bestModel
    println(s"GBT Model was fit using parameters: ${gbtBestModel.parent.extractParamMap}")
    //check performance on test data
    val gbtPred = gbtBestModel.transform(valData)
    val gbtEval = new BinaryClassificationEvaluator().setLabelCol("label")
    println("ROC using GBT: "+evaluator.evaluate(gbtPred))  
    spark.stop()
  }
}