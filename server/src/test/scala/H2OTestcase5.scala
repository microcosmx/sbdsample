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

class H2OTestcase5 extends FlatSpec with Matchers with BeforeAndAfterAll with TestKitBase {

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
            // Common imports from H2O and Sparks
            import _root_.hex.deeplearning.DeepLearning
            import _root_.hex.deeplearning.DeepLearningModel.DeepLearningParameters
            import org.apache.spark.h2o.{DoubleHolder, H2OContext, H2OFrame}
            import org.apache.spark.rdd.RDD
            import org.apache.spark.sql.SQLContext
            import org.apache.spark.{SparkContext, SparkFiles}
            import water.app.SparkContextSupport
            
            import org.apache.spark.h2o._
            import org.apache.spark.examples.h2o._
            
            import java.io.File
            
            val path = "data/examples/smalldata/allyears2k_headers.csv"
        
            // Run H2O cluster inside Spark cluster
            val h2oContext = H2OContext.getOrCreate(sc)
            import h2oContext._
            import h2oContext.implicits._
        
            //
            // Load H2O from CSV file (i.e., access directly H2O cloud)
            // Use super-fast advanced H2O CSV parser !!!
            val airlinesData = new H2OFrame(new File("data/examples/smalldata/allyears2k_headers.csv.gz"))
        
            //
            // Use H2O to RDD transformation
            //
            val airlinesTable : RDD[Airlines] = asRDD[Airlines](airlinesData)
            println(s"\n===> Number of all flights via RDD#count call: ${airlinesTable.count()}\n")
            println(s"\n===> Number of all flights via H2O#Frame#count: ${airlinesData.numRows()}\n")
        
            //
            // Filter data with help of Spark SQL
            //
        
            val sqlContext = new SQLContext(sc)
            import sqlContext.implicits._ // import implicit conversions
            airlinesTable.toDF.registerTempTable("airlinesTable")
        
            // Select only interesting columns and flights with destination in SFO
            val query = "SELECT * FROM airlinesTable WHERE Dest LIKE 'SFO'"
            val result : H2OFrame = sqlContext.sql(query) // Using a registered context and tables
            println(s"\n===> Number of flights with destination in SFO: ${result.numRows()}\n")
        
            //
            // Run Deep Learning
            //
        
            println("\n====> Running DeepLearning on the result of SQL query\n")
            // Training data
            val train = result('Year, 'Month, 'DayofMonth, 'DayOfWeek, 'CRSDepTime, 'CRSArrTime,
              'UniqueCarrier, 'FlightNum, 'TailNum, 'CRSElapsedTime, 'Origin, 'Dest,
              'Distance, 'IsDepDelayed )
            train.replace(train.numCols()-1, train.lastVec().toCategoricalVec)
            train.update()
        
            // Configure Deep Learning algorithm
            val dlParams = new DeepLearningParameters()
            dlParams._train = train
            dlParams._response_column = 'IsDepDelayed
        
            val dl = new DeepLearning(dlParams)
            val dlModel = dl.trainModel.get
        
            //
            // Use model for scoring
            //
            println("\n====> Making prediction with help of DeepLearning model\n")
            val predictionH2OFrame = dlModel.score(result)('predict)
            val predictionsFromModel = asRDD[DoubleHolder](predictionH2OFrame).collect.map ( _.result.getOrElse("NaN") )
            println(predictionsFromModel.mkString("\n===> Model predictions: ", ", ", ", ...\n"))
        

        }
        catch {
            case t: Throwable =>
                t.printStackTrace()
        }
    }


}
