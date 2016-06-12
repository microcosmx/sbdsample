import java.lang._
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.{LinearRegressionModel,RidgeRegressionModel,LassoModel}
import org.apache.spark.mllib.regression.{RidgeRegressionWithSGD,LassoWithSGD,LinearRegressionWithSGD}
import org.apache.spark.mllib.linalg.Vectors

import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.util.MLUtils

import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics


case class MLSample(
    sc: SparkContext,
    sqlContext: SQLContext)
{
  
    def mlLinear() = {
      import org.apache.spark.ml.regression.LinearRegression

      // Load training data
      val training = sqlContext.read.format("libsvm")
        //.load("data/mllib/sample_linear_regression_data.txt")
        .load("data/lpsa3.data")
        
      training.show()
      
      val lr = new LinearRegression()
        .setMaxIter(100)
        //.setRegParam(0.3)
        .setElasticNetParam(0.8)
      
      // Fit the model
      val lrModel = lr.fit(training)
      
      //prediction
      val predictions = lrModel.transform(training)
      println("------------predictions----------------")
      predictions.show()
      
      //cross-validation
      val Array(trainingData, testData) = training.randomSplit(Array(0.8, 0.2))
      val lr2 = new LinearRegression()
        .setMaxIter(100)
        .setRegParam(0.3)
        .setElasticNetParam(0.8)
      val lrModel2 = lr2.fit(trainingData)
      val predictions2 = lrModel2.transform(testData)
      println("-------------predictions 2---------------")
      predictions2.show()
      
      // Print the coefficients and intercept for linear regression
      println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
      
      // Summarize the model over the training set and print out some metrics
      val trainingSummary = lrModel.summary
      println(s"numIterations: ${trainingSummary.totalIterations}")
      println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
      trainingSummary.residuals.show()
      println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
      println(s"r2: ${trainingSummary.r2}")
      
    }
    
    def mlDTree(data:DataFrame,trainingData:DataFrame,testData:DataFrame) = {
      import org.apache.spark.ml.Pipeline
      import org.apache.spark.ml.regression.DecisionTreeRegressor
      import org.apache.spark.ml.regression.DecisionTreeRegressionModel
      import org.apache.spark.ml.feature.VectorIndexer
      import org.apache.spark.ml.evaluation.RegressionEvaluator
      
      // Load the data stored in LIBSVM format as a DataFrame.
//      val data = sqlContext.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")
 
      // Automatically identify categorical features, and index them.
      // Here, we treat features with > 4 distinct values as continuous.
      val featureIndexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexedFeatures")
        .setMaxCategories(100)
        .fit(data)
      
      // Split the data into training and test sets (30% held out for testing)
//      val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
      
      // Train a DecisionTree model.
      val dt = new DecisionTreeRegressor()
        //.setMaxDepth(10)
        .setMaxBins(100)
        .setLabelCol("label")
        .setFeaturesCol("indexedFeatures")
      
      // Chain indexer and tree in a Pipeline
      val pipeline = new Pipeline()
        .setStages(Array(featureIndexer, dt))
      
      // Train model.  This also runs the indexer.
      val model = pipeline.fit(trainingData)
      
      // Make predictions.
      val predictions = model.transform(testData)
      
      // Select example rows to display.
//      predictions.select("prediction", "label", "features").show(100)
      
      // Select (prediction, true label) and compute test error
      val evaluator = new RegressionEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMetricName("rmse")
      val rmse = evaluator.evaluate(predictions)
      //println("Root Mean Squared Error (RMSE) on test data = " + rmse)
      
//      val treeModel = model.stages(1).asInstanceOf[DecisionTreeRegressionModel]
//      println("Learned regression tree model:\n" + treeModel.toDebugString)
      
      predictions
    }
    
    def mlForest(data:DataFrame) = {
      import org.apache.spark.ml.Pipeline
      import org.apache.spark.ml.evaluation.RegressionEvaluator
      import org.apache.spark.ml.feature.VectorIndexer
      import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
      
      // Load and parse the data file, converting it to a DataFrame.
//      val data = sqlContext.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")
      
      // Automatically identify categorical features, and index them.
      // Set maxCategories so features with > 4 distinct values are treated as continuous.
      val featureIndexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexedFeatures")
        .setMaxCategories(4)
        .fit(data)
      
      // Split the data into training and test sets (30% held out for testing)
//      val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
      val Array(trainingData, testData) = Array(data,data)
        
      // Train a RandomForest model.
      val rf = new RandomForestRegressor()
        .setLabelCol("label")
        .setFeaturesCol("indexedFeatures")
      
      // Chain indexer and forest in a Pipeline
      val pipeline = new Pipeline()
        .setStages(Array(featureIndexer, rf))
      
      // Train model.  This also runs the indexer.
      val model = pipeline.fit(trainingData)
      
      // Make predictions.
      val predictions = model.transform(testData)
      
      // Select example rows to display.
      predictions.select("prediction", "label", "features").show(20)
      
      // Select (prediction, true label) and compute test error
      val evaluator = new RegressionEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMetricName("rmse")
      val rmse = evaluator.evaluate(predictions)
      println("Root Mean Squared Error (RMSE) on test data = " + rmse)
      
      val rfModel = model.stages(1).asInstanceOf[RandomForestRegressionModel]
      println("Learned regression forest model:\n" + rfModel.toDebugString)
    }
    
    def mlexec() = {
      // Create a dense vector (1.0, 0.0, 3.0).
      val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
      // Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
      val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
      // Create a sparse vector (1.0, 0.0, 3.0) by specifying its nonzero entries.
      val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))
    }
    
    def mlKMeans() = {
      // 屏蔽不必要的日志显示在终端上
      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
      // 装载数据集
      val data = sc.textFile("data/kmeans_data.txt", 1)
      val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble)))
      // 将数据集聚类，2个类，20次迭代，进行模型训练形成数据模型
      val numClusters = 2
      val numIterations = 20
      val model = KMeans.train(parsedData, numClusters, numIterations)
      // 打印数据模型的中心点
      println("Cluster centers:")
      for (c <- model.clusterCenters) {
        println("  " + c.toString)
      }
      // 使用误差平方之和来评估数据模型
      val cost = model.computeCost(parsedData)
      println("Within Set Sum of Squared Errors = " + cost)
      // 使用模型测试单点数据
      println("Vectors 0.2 0.2 0.2 is belongs to clusters:" + model.predict(Vectors.dense("0.2 0.2 0.2".split(' ').map(_.toDouble))))
      println("Vectors 0.25 0.25 0.25 is belongs to clusters:" + model.predict(Vectors.dense("0.25 0.25 0.25".split(' ').map(_.toDouble))))
      println("Vectors 8 8 8 is belongs to clusters:" + model.predict(Vectors.dense("8 8 8".split(' ').map(_.toDouble))))
      // 交叉评估1，只返回结果
      val testdata = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble)))
      val result1 = model.predict(testdata)
      result1.saveAsTextFile("data/result_kmeans1")
      // 交叉评估2，返回数据集和结果
      val result2 = data.map {
        line =>
          val linevectore = Vectors.dense(line.split(' ').map(_.toDouble))
          val prediction = model.predict(linevectore)
          line + " " + prediction
      }.saveAsTextFile("data/result_kmeans2")
    }
    
    
    def LinearRegressionTest1() = {
      // Load and parse the data
      val data = sc.textFile("data/lpsa1.data")
      val parsedData = data.map { line =>
        val parts = line.split(',')
        LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
      }.cache()
      
      // Building the model
      val numIterations = 100000
      val stepSize = 0.1
      val model = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize)
      
      //data 2 for predict
      val data2 = sc.textFile("data/lpsa2.data")
      val parsedData2 = data2.map { line =>
        val parts = line.split(',')
        LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
      }.cache()
      // Evaluate model on training examples and compute training error
      val valuesAndPreds = parsedData2.map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
      }
      
      import spray.json._
      import DefaultJsonProtocol._
      println("------------------predict-----------------")
      println(valuesAndPreds.collect.toJson)
      println("------------------means-----------------")
      val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
      println("training Mean Squared Error = " + MSE)
      
      // Save and load model
      //model.save(sc, "myModelPath")
      //val sameModel = LinearRegressionModel.load(sc, "myModelPath")
    }
    
    def LinearRegressionTest2() = {
      import org.apache.spark.ml.regression.LinearRegression

      // Load training data
      val training = sqlContext.read.format("libsvm")
        .load("data/mllib/sample_linear_regression_data.txt")
      
      val lr = new LinearRegression()
        .setMaxIter(10)
        .setRegParam(0.3)
        .setElasticNetParam(0.8)
      
      // Fit the model
      val lrModel = lr.fit(training)
      
      // Print the coefficients and intercept for linear regression
      println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
      
      // Summarize the model over the training set and print out some metrics
      val trainingSummary = lrModel.summary
      println(s"numIterations: ${trainingSummary.totalIterations}")
      println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
      trainingSummary.residuals.show()
      println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
      println(s"r2: ${trainingSummary.r2}")
    }
    
    
    def LinearRegressionTest(result:DataFrame) = {
      // Load and parse the data
//      val data = sc.textFile("data/lpsa.data",1)
//      val parsedData = data.map { line =>
//        val parts = line.split(',')
//        LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
//      }.cache()
      
      val parsedData = result.map { row => 
//          val artist_id = row(0).toString().toDouble
          val plays = row(1).toString().toDouble
          val ds = row(2).toString().toDouble
          LabeledPoint(plays, Vectors.dense(Array(ds)))
      }.cache()
      
//      LinearRegressionWithSGD.train(input, numIterations, stepSize, miniBatchFraction, initialWeights)
      
      // Building the model
      val numIterations = 1000
      val stepSize = 0.0001
      val model = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize)
//      val model1 = RidgeRegressionWithSGD.train(parsedData, numIterations);//L2
//      val model2 = LassoWithSGD.train(parsedData, numIterations);//L1
      
      // Evaluate model on training examples and compute training error
      val valuesAndPreds = parsedData.map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
      }
      
//      import spray.json._
//      import DefaultJsonProtocol._
//      println("------------------predict-----------------")
//      println(valuesAndPreds.collect.toJson)
//      println("------------------means-----------------")
      
      val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
      println("training Mean Squared Error = " + MSE)
      
      // Save and load model
      //model.save(sc, "myModelPath")
      //val sameModel = LinearRegressionModel.load(sc, "myModelPath")
      
      valuesAndPreds
    }
    
    
    def LogisticRegressionTest(result:DataFrame) = {
      // Load training data in LIBSVM format.
      val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
      
//      val data = result.map { row => 
////        val artist_id = row(0).toString().toDouble
//          val plays = row(1).toString().toDouble
//          val ds = row(2).toString().toDouble
//          LabeledPoint(plays, Vectors.dense(ds))
//      }.cache()

      // Split data into training (60%) and test (40%).
      val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
      val training = splits(0).cache()
      val test = splits(1)

      // Run training algorithm to build the model
      val model = new LogisticRegressionWithLBFGS()
        .setNumClasses(10)
        .run(training)

      // Compute raw scores on the test set.
      val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
      }

      // Get evaluation metrics.
      val metrics = new MulticlassMetrics(predictionAndLabels)
      val precision = metrics.precision
      println("Precision = " + precision)

      // Save and load model
//      model.save(sc, "myModelPath")
//      val sameModel = LogisticRegressionModel.load(sc, "myModelPath")
      predictionAndLabels
    }
    
    //Linear Support Vector Machines
    def LinearSVM(result:DataFrame)={
      // Load training data in LIBSVM format.
      val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")

      // Split data into training (60%) and test (40%).
      val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
      val training = splits(0).cache()
      val test = splits(1)

      // Run training algorithm to build the model
      val numIterations = 100
      val model = SVMWithSGD.train(training, numIterations)

      // Clear the default threshold.
      model.clearThreshold()

      // Compute raw scores on the test set.
      val scoreAndLabels = test.map { point =>
        val score = model.predict(point.features)
        (score, point.label)
      }

      // Get evaluation metrics.
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      val auROC = metrics.areaUnderROC()

      println("Area under ROC = " + auROC)

//      // Save and load model
//      model.save(sc, "myModelPath")
//      val sameModel = SVMModel.load(sc, "myModelPath")
      scoreAndLabels
      
    }
}