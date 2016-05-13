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

class AliyunDataset2 extends FlatSpec with Matchers with BeforeAndAfterAll with TestKitBase {

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
            
            val lines = env.sc.textFile("data/mars_tianchi_songs.csv")
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
            
            //join
           //action_type
           // 1 for plays
           // 2 for downloads
           // 3 for favors
            val result1 = songDF
                  .filter(songDF("artist_id") === "03c6699ea836decbc5c8fc2dbae7bd3b")
                  .join(uactDF, songDF("song_id") === uactDF("song_id"))
                  //.filter(songDF("publish_time") >= uactDF("Ds"))
                  .filter(uactDF("action_type") === 1)
                  .groupBy(songDF("artist_id") as "artist_id", uactDF("Ds") as "Ds")
                  .agg(count("action_type") as "Plays", max("song_init_plays") as "initCount",max("publish_time") as "pub_date")
                  .sort("Ds").sort("artist_id")
                  .select("artist_id","Plays","Ds", "initCount","pub_date")
                  .sort("Ds")

              result1.printSchema()
              result1.show()
              
          

            import java.io._
            val writer = new PrintWriter(new File("data/result.csv"))
            result1.collect.foreach { 
               case Row(aid: String,play: String,ds: String,ic: String,pub: String) =>
                  writer.println(s"$aid,$play,$ds,$ic,$pub")
               case Row(aid: String,play: Long,ds: String,ic: String,pub: String) =>
                  val dss = ds.substring(0, 4) + '/' + ds.substring(4, 6) + '/' + ds.substring(6)
                  val pubs = pub.substring(0, 4) + '/' + pub.substring(4, 6) + '/' + pub.substring(6)
                  writer.println(s"$aid,$play,$dss,$ic,$pubs")
               
               case _ => 
            }
            writer.close()
            
            //.saveAsTextFile("data/mars_tianchi_artist_plays_predict")
            //result1.rdd.repartition(1).saveAsTextFile("data/mars_tianchi_artist_plays_predict.csv")
            //result1.rdd.coalesce(1,true).saveAsTextFile("data/mars_tianchi_artist_plays_predict.csv")
            
            //env.ml.LinearRegressionTest(result1)
           

           
            
        }
        catch {
            case t: Throwable =>
                t.printStackTrace()
        }
    }


}
