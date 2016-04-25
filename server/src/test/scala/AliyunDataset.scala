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

class AliyunDataset extends FlatSpec with Matchers with BeforeAndAfterAll with TestKitBase {

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
        val ml = MLSample(sc)
        val ss = SStream(sc)
        val env = Env(system, cfg, fs, jdbc, ml, sc, sqlContext)

        try{
            import env.sqlContext.implicits._
            
            val lines = env.sc.textFile("data/mars_tianchi_songs.csv")
            .filter { line => 
              line.contains("03c6699ea836decbc5c8fc2dbae7bd3b")}
            .map(_.split(",", -1).map(_.trim)).collect
            val songs = lines
              .map( line =>
                mars_tianchi_songs(
                  song_id = line(0).toString, 
                  artist_id = line(1).toString, 
                  publish_time = line(2).toString, 
                  song_init_plays = line(3).toString, 
                  Language = line(4).toString, 
                  Gender = line(5).toString)
              ).toSeq
            val songDF = env.sc.parallelize(songs).toDF
            songDF.printSchema()
            songDF.limit(10).show()
 
            val mtua = env.sc.textFile("data/mars_tianchi_user_actions.csv")
            val mtua_rdd = mtua.map(_.split(",", -1).map(_.trim)).map { line =>
                mars_tianchi_user_actions(
                  user_id = line(0).toString, 
                  song_id = line(1).toString, 
                  gmt_create = line(2).toString, 
                  action_type = line(3).toString, 
                  Ds = line(4).toString)}
           val uactDF = mtua_rdd.toDF
           uactDF.printSchema()
           uactDF.show()
//            testDF.printSchema()
//           testDF.show(5)
//            mtua.filter { line =>
//              true  
//            }
//            val ualines = mtua
//            .take(200000)
//            .map(_.split(",", -1).map(_.trim))
////            .collect
//            val uacts = ualines
//              .map( line =>
//                mars_tianchi_user_actions(
//                  user_id = line(0).toString, 
//                  song_id = line(1).toString, 
//                  gmt_create = line(2).toString, 
//                  action_type = line(3).toString, 
//                  Ds = line(4).toString)
//              ).toSeq
//            uactDF = env.sc.parallelize(uacts).toDF
//            uactDF.printSchema()
//            uactDF.show()        
            
            //join
            val result1 = songDF
                  .join(uactDF, songDF("song_id") === uactDF("song_id"))
                  .filter(songDF("publish_time") >= uactDF("Ds"))
                  .filter(uactDF("action_type") === 1)
                  .groupBy(songDF("artist_id") as "artist_id", uactDF("Ds") as "Ds")
                  .agg(count("action_type") as "Plays", max("song_init_plays") as "initCount")
                  .sort("Ds").sort("artist_id")
                  .select("artist_id","Plays","Ds")

              result1.printSchema()
              result1.show()
            
//            result1.rdd.saveAsTextFile("data/mars_tianchi_artist_plays_predict")
            //result1.rdd.repartition(1).saveAsTextFile("data/mars_tianchi_artist_plays_predict.csv")
            //result1.rdd.coalesce(1,true).saveAsTextFile("data/mars_tianchi_artist_plays_predict.csv")
            
            //translate  date to num    
            val tmp0 = result1.map { row => 
                val artist_id = row.get(0).toString()
                val plays = row.get(1).toString()
                val ds = row.get(2).toString()
                
                //count the number of days since 2015-03-01
                val sdf=new SimpleDateFormat("yyyyMMdd")
                
                val begin = sdf.parse("20150301")
                val end = sdf.parse(ds)
                val between:Long=(end.getTime-begin.getTime)/1000
                val hour:Float=between.toFloat/3600
                val days:Int = hour.toInt/24
                
                Row(artist_id,plays,days.toString)
              }
           
            val schemaString = "artist_id Plays Ds"
            val schema = StructType(schemaString.split(" ").map(fieldName=>StructField(fieldName,StringType,true)))
            val tmp1 = sqlContext.createDataFrame(tmp0, schema)
             tmp1.printSchema()
             tmp1.show()
             tmp1.rdd.saveAsTextFile("data/mars_tianchi_artist_plays_predict")
            
  
            //machine learning sample
            val predictResult = env.ml.LinearRegressionTest(tmp1)
            predictResult.saveAsTextFile("data/linearResult")
            
//             val predictResult2 = env.ml.LogisticRegressionTest(tmp1)
//             predictResult2.saveAsTextFile("data/logisticResult")
            
//             val predictResult3 = env.ml.LinearSVM(tmp1)
//             predictResult3.saveAsTextFile("data/LinearSVMResult")
            
            
        }
        catch {
            case t: Throwable =>
                t.printStackTrace()
        }
    }


}
