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
import org.apache.spark.sql.types.{StructType,StructField,StringType}
import akka.testkit.TestKitBase
import org.apache.spark.repl._


import scala.Option.option2Iterable
import scala.collection.mutable.ListBuffer
import scala.tools.nsc.util.FailedInterrupt
import scala.tools.refactoring.Refactoring
import scala.tools.refactoring.common.Change
import scala.tools.refactoring.common.NewFileChange
import scala.tools.refactoring.common.TextChange
import scala.tools.refactoring.util.CompilerProvider
import scala.tools.refactoring.common.InteractiveScalaCompiler
import scala.tools.refactoring.common.Selections
import language.{ postfixOps, reflectiveCalls }
import scala.tools.refactoring.common.NewFileChange
import scala.tools.refactoring.common.RenameSourceFileChange
import scala.tools.refactoring.implementations.Rename
import scala.tools.refactoring.common.TracingImpl
import scala.tools.refactoring.util.UniqueNames

import scala.tools.refactoring._
import scala.tools.refactoring.util._
import scala.tools.refactoring.analysis._
import scala.tools.refactoring.common._
import scala.tools.refactoring.implementations._
import scala.tools.refactoring.sourcegen._
import scala.tools.refactoring.transformation._
            
            

class SparkTestcase2 extends FlatSpec with Matchers with BeforeAndAfterAll with TestKitBase {

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
//        conf.set("spark.repl.class.uri",H2OInterpreter.classServerUri)

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
            import env.sqlContext.implicits._
            
            import scala.reflect.internal._
            import scala.reflect.internal.util._
            val global = (new CompilerInstance).compiler
            import global._
            
            val src = """
              package shadowing
          
              object TheShadow {
                val i = 1
          
                def method: Unit = {
                  val i = ""
                  ()
                }
              }
          
              class Xyz(xyzxyz: Long) {
          
                def method: Unit = {
                  val xyzxyz = ""
                  val i = xyzxyz
                  ()
                }
              }
          
              class Z {
                import TheShadow._
          
                val i = Nil
              }"""
            
            
            val file = new BatchSourceFile(UniqueNames.basename(), src)
            

            println("-----------------")
            println(sc.parallelize(1 to 1000).count)
            
        }
        catch {
            case t: Throwable =>
                t.printStackTrace()
        }
    }
    


}
