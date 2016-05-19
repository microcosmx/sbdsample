import akka.actor._
import akka.io.IO
import akka.pattern._
import akka.routing._
import akka.util.Timeout
import org.apache.hadoop.fs._
import org.apache.avro._
import org.apache.avro.file._
import org.apache.avro.reflect._
import org.apache.hadoop.fs._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import scala.collection.JavaConversions._
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import spray.can.Http
import spray.json._
import DefaultJsonProtocol._
import org.scalatest._
import scala.annotation._
import scala.math.Ordered.orderingToOrdered
import scala.math.Ordering.Implicits._
import scala.util.hashing._
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.mllib.classification.{SVMModel,SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType,StructField,StringType};

import akka.testkit.TestKitBase

class H2OTestcase2 extends FlatSpec with Matchers with BeforeAndAfterAll with TestKitBase {

    implicit lazy val system = ActorSystem()
    implicit val timeout: Timeout = 1.minute

    var fs: org.apache.hadoop.fs.FileSystem = null
    var sc: org.apache.spark.SparkContext = null
    var sqlContext: org.apache.spark.sql.SQLContext = null

    override def beforeAll() {
        import org.apache.log4j.Logger
        import org.apache.log4j.Level
        Logger.getLogger("org").setLevel(Level.WARN)
        Logger.getLogger("akka").setLevel(Level.WARN)
        Logger.getLogger("parquet.hadoop").setLevel(Level.WARN)

        fs = FileSystem.get(new org.apache.hadoop.conf.Configuration)

        val conf = new org.apache.spark.SparkConf
        conf.set("spark.master", "local[*]")
        //conf.set("spark.master", "spark://192.168.20.17:7070")
        conf.set("spark.app.name", "Aliyun")
        conf.set("spark.ui.port", "55555")
        conf.set("spark.default.parallelism", "10")
        conf.set("spark.sql.shuffle.partitions", "10")

        sc = new org.apache.spark.SparkContext(conf)
        sqlContext = new org.apache.spark.sql.SQLContext(sc)
    }

    override def afterAll() {
        sc.stop()
        fs.close()
        system.shutdown()
    }

    it should "run it" in {
        val cfg = new Config("conf/server.properties")
        
        val jdbc = null
        val ml = MLSample(sc,sqlContext)
        val ss = SStream(sc)
        val env = Env(system, cfg, fs, jdbc, ml, sc, sqlContext)

        try{
            import org.apache.spark.h2o._
            import org.apache.spark.examples.h2o._
            val h2oContext = H2OContext.getOrCreate(sc)
            val path = "data/examples/smalldata/test.csv"
            val prostateText = sc.textFile(path)
            val prostateRDD = prostateText.map(_.split(",")).map(row => Row(row(0).toDouble,row(1).toDouble))
            val schemaString = "y x"
            import org.apache.spark.sql.types._
            val schema = StructType(schemaString.split(" ").map(fieldName=>StructField(fieldName,DoubleType,true)))
            val df = sqlContext.createDataFrame(prostateRDD, schema)
            
            
            import h2oContext._
            val train: H2OFrame = h2oContext.asH2OFrame(df,  Some("h2oframe"))
            println(train.toString())
            
            import _root_.hex.tree.gbm.GBM
            import _root_.hex.tree.gbm.GBMModel.GBMParameters
            val gbmParams = new GBMParameters()
            gbmParams._train = train.key
            gbmParams._response_column = "y"
            gbmParams._ntrees = 10
            val gbmModel = new GBM(gbmParams).trainModel.get
            println("---------------->print Model")
            println(gbmModel)
            println("---------------->print output")
            println(gbmModel._output)
            println("---------------->print auc")
            println(gbmModel.auc)
            
            val predictTable = gbmModel.score(train)
            println("---------------->print predictTable")
            println(predictTable)
            

        }
        catch {
            case t: Throwable =>
                t.printStackTrace()
        }
    }


}
