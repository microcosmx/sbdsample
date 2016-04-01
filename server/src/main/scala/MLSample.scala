
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
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors


case class MLSample(
    sc: SparkContext)
{
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
    
    
    def LinearRegressionTest() = {
      // Load and parse the data
      val data = sc.textFile("data/mllib/ridge-data/lpsa.data",1)
      val parsedData = data.map { line =>
        val parts = line.split(',')
        LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
      }.cache()
      
      // Building the model
      val numIterations = 100
      val stepSize = 0.00000001
      val model = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize)
      
      // Evaluate model on training examples and compute training error
      val valuesAndPreds = parsedData.map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
      }
      println("------------------predict-----------------")
      println(valuesAndPreds)
      
      val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
      println("training Mean Squared Error = " + MSE)
      
      // Save and load model
      //model.save(sc, "myModelPath")
      //val sameModel = LinearRegressionModel.load(sc, "myModelPath")
      
      valuesAndPreds
    }
}