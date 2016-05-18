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
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar
import org.apache.spark.mllib.classification.{SVMModel,SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType,StructField,StringType};

import akka.testkit.TestKitBase

class AliyunDataset3_H2O extends FlatSpec with Matchers with BeforeAndAfterAll with TestKitBase {

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
        //conf.set("spark.master", "spark://192.168.20.17:7077")
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
            val songDF_base = env.sc.parallelize(songs).toDF
            songDF_base.printSchema()
            songDF_base.limit(10).show()
 
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
           //filter
           val artist_id_ = "03c6699ea836decbc5c8fc2dbae7bd3b"
           
           val songDF = songDF_base.filter(songDF_base("artist_id") === artist_id_)
            val result1 = songDF
                  .join(uactDF, songDF("song_id") === uactDF("song_id"))
                  //.filter(songDF("publish_time") >= uactDF("Ds"))
                  .filter(uactDF("action_type") === 1)
                  .groupBy(songDF("artist_id") as "artist_id", uactDF("Ds") as "Ds")
                  .agg(count("action_type") as "Plays", sum("song_init_plays") as "initCount", max("publish_time") as "pubDate", 
                      avg("Language") as "lang", avg("Gender") as "gender", sum("gmt_create") as "pSpan")
                  .sort("Ds").sort("artist_id")
                  .select("artist_id","Plays","Ds","initCount", "pubDate","lang","gender","pSpan")
                  .sort("Ds")
              
             val result2 = songDF
                  .join(uactDF, songDF("song_id") === uactDF("song_id"))
                  //.filter(songDF("publish_time") >= uactDF("Ds"))
                  .filter(uactDF("action_type") === 2)
                  .groupBy(songDF("artist_id") as "artist_id", uactDF("Ds") as "Ds")
                  .agg(count("action_type") as "Downloads")
                  .sort("Ds").sort("artist_id")
                  .select("Downloads","Ds")
                  .withColumnRenamed("Ds", "Ds2")
                  .sort("Ds2")
              
              val result3 = songDF
                  .join(uactDF, songDF("song_id") === uactDF("song_id"))
                  //.filter(songDF("publish_time") >= uactDF("Ds"))
                  .filter(uactDF("action_type") === 3)
                  .groupBy(songDF("artist_id") as "artist_id", uactDF("Ds") as "Ds")
                  .agg(count("action_type") as "Favors")
                  .sort("Ds").sort("artist_id")
                  .select("Favors","Ds")
                  .withColumnRenamed("Ds", "Ds3")
                  .sort("Ds3")
              
              val agg_result = result1
              .join(result2, result1("Ds")===result2("Ds2"),"left_outer")
              .join(result3, result1("Ds")===result3("Ds3"),"left_outer") 
              .sort("Ds")
              .select(
                  "artist_id",
                  "Plays",
                  "Downloads",
                  "Favors",
                  "Ds",
                  "initCount",
                  "pubDate",
                  "lang",
                  "gender",
                  "pSpan")
                  
              agg_result.printSchema()
              agg_result.show()
            
            //translate  date to num   
            import org.apache.spark.mllib.linalg.{Vector, Vectors}
            val tmp0 = agg_result.map { row => 
                val artist_id = row.get(0).toString()
                val plays = row.get(1).toString().toDouble
                val downloads = row.get(2).toString().toDouble
                var favors:Double = 0
                if(row.get(3) == null)
                 favors = 0
                else
                  favors = row.get(3).toString().toDouble
                
                val ds = row.get(4).toString()
                val initCount = row.get(5).toString().toDouble
                val pubDate = row.get(6).toString()
                
                var lang = row.get(7).toString().toDouble
                val langMap = Map(
                    Math.abs(lang-1)->1,
                    Math.abs(lang-2)->2,
                    Math.abs(lang-3)->3,
                    Math.abs(lang-4)->4,
                    Math.abs(lang-11)->11,
                    Math.abs(lang-12)->12,
                    Math.abs(lang-14)->14,
                    Math.abs(lang-100)->100
                )
                val langkey = langMap.keySet.toSeq.sortBy(x=>x).head
                lang = langMap(langkey)
                
                var gender = row.get(8).toString().toDouble
                if(gender < 1.5)
                  gender = 1
                else if(gender > 2.5)
                  gender = 3
                else 
                  gender = 2
                  
                val pSpan = row.get(9).toString().toDouble
                
                //count the number of days since 2015-03-01
                val sdf=new SimpleDateFormat("yyyyMMdd")
                
                val begin = sdf.parse("20150301")
                val end = sdf.parse(ds)
                val between:Long=(end.getTime-begin.getTime)/1000
                val hour:Float=between.toFloat/3600
                val days:Long = hour.toLong/24
                val date_feature = days/1
                
                val begin_pub = sdf.parse("20170101")
                val end_pub = sdf.parse(pubDate)
                val between_pub:Long=(begin_pub.getTime-end_pub.getTime)/1000
                val hour_pub:Float=between_pub.toFloat/3600
                val days_pub:Long = hour_pub.toLong/24
                val pubDate_feature = days_pub/180
                
                artistId_plays_ds(
                    artist_id= artist_id,
                    Plays= plays,
                    Downloads = downloads,
                    Favors= favors,
                    Ds=date_feature.toDouble,
                    initCount=initCount,
                    pubDate=pubDate_feature.toDouble,
                    lang=lang,
                    gender=gender,
                    pSpan=pSpan
                )
                
              }
          
           val seq = tmp0.collect().toSeq
           val data = sc.parallelize(seq).toDF
           
           //println("----------data size-----------")
           //println(s"----------${data.count}-----------")
           
           val dataRDD = data.map { row => 
                val artist_id = row.get(0).toString()
                val plays = row.get(1).toString().toDouble
                val downloads = row.get(2).toString().toDouble
                val favors = row.get(3).toString().toDouble
                val ds = row.get(4).toString().toDouble
                val initCount = row.get(5).toString().toDouble
                val pubDate = row.get(6).toString().toDouble
                
                val lang = row.get(7).toString().toDouble
                val gender = row.get(8).toString().toDouble
                
                val pSpan = row.get(9).toString().toDouble
                
                val label = plays
                val features = Vectors.dense(Array(downloads,favors,ds,initCount,pubDate,lang,gender))
                params(
                  label=label,
                  features=features
                )
           }
           
           val trainingRDD = data.filter(data("Ds")<200).map { row => 
                val artist_id = row.get(0).toString()
                val plays = row.get(1).toString().toDouble
                val downloads = row.get(2).toString().toDouble
                val favors = row.get(3).toString().toDouble
                val ds = row.get(4).toString().toDouble
                val initCount = row.get(5).toString().toDouble
                val pubDate = row.get(6).toString().toDouble
                
                val lang = row.get(7).toString().toDouble
                val gender = row.get(8).toString().toDouble
                
                val pSpan = row.get(9).toString().toDouble
                
                val label = plays
                val features = Vectors.dense(Array(downloads,favors,ds,initCount,pubDate,lang,gender))
                params(
                  label=label,
                  features=features
                )
           
           }
           
           var ds_begin = 180
           val testRDD = data.filter("Ds > 60 and Ds % 2 = 0").map { row => 
                val artist_id = row.get(0).toString()
                val plays = row.get(1).toString().toDouble
                val downloads = row.get(2).toString().toDouble
                val favors = row.get(3).toString().toDouble
                //val ds = row.get(4).toString().toDouble
                ds_begin += 1
                val ds = ds_begin
                val initCount = row.get(5).toString().toDouble
                val pubDate = row.get(6).toString().toDouble
                
                val lang = row.get(7).toString().toDouble
                val gender = row.get(8).toString().toDouble
                
                val pSpan = row.get(9).toString().toDouble
                
                val label = plays
                val features = Vectors.dense(Array(downloads,favors,ds,initCount,pubDate,lang,gender))
                params(
                  label=label,
                  features=features
                )
           
           }
           val data2 = sc.parallelize(dataRDD.collect().toSeq).toDF
           val trainingData = sc.parallelize(trainingRDD.collect().toSeq).toDF
           val testData = sc.parallelize(testRDD.collect().toSeq).toDF
           
           data2.printSchema()
           data2.show()
           
           
           val predictions = env.ml.mlDTree(data2,trainingData,testData)
           println("-----------result-------------")
           predictions.printSchema()
           predictions.select("prediction", "label", "features").show(100)
           
//           env.ml.mlForest(data2)
           
            import java.io._
            val writer = new PrintWriter(new File("data/regression_result.csv"))
            val sdf = new SimpleDateFormat("yyyyMMdd")
            val begin = sdf.parse("20150901")
            val cal = new java.util.GregorianCalendar
            cal.setTime(begin)
            predictions.limit(60).select("prediction").collect.foreach { 
               case Row(predic: String) =>
                  //println(predic)
               case Row(predic: Double) =>
                 val ds_ = sdf.format(cal.getTime())
                 val predic_ = Math.floor(predic).toInt
                  writer.println(s"$artist_id_,$predic_,$ds_")
                  cal.add(Calendar.DATE, 1)
               case _ => 
            }
            writer.close()
           
            
        }
        catch {
            case t: Throwable =>
                t.printStackTrace()
        }
    }


}
