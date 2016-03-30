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
            
            val lines = sc.textFile("data/mars_tianchi_songs.csv").map(_.split(",", -1).map(_.trim)).collect
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
            
            //lines = sc.textFile("data/mars_tianchi_user_actions.csv").map(_.split(",", -1).map(_.trim)).collect
            val ualines = sc.textFile("data/mars_tianchi_user_actions.csv").take(20).map(_.split(",", -1).map(_.trim))//.collect
            val uacts = ualines
              .map( line =>
                mars_tianchi_user_actions(
                  user_id = line(0).toString, 
                  song_id = line(1).toString, 
                  gmt_create = line(2).toString, 
                  action_type = line(3).toString, 
                  Ds = line(4).toString)
              ).toSeq
            val uactDF = env.sc.parallelize(uacts).toDF
            uactDF.printSchema()
            uactDF.limit(10).show()
        }
        catch {
            case t: Throwable =>
                t.printStackTrace()
        }
    }
}
