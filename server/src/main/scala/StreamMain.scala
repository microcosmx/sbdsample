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

object StreamMain extends App {
    def initFileSystem(sc: SparkContext, props:Map[String,String]) = {
        val conf = sc.hadoopConfiguration
        props.foreach(Function.tupled(conf.set))
        FileSystem.enableSymlinks()
        FileSystem.get(conf)
    }

    def initSparkContext(props:Map[String,String]) = {
        val conf = new org.apache.spark.SparkConf()
        props.foreach(Function.tupled(conf.set))
        val sc = new org.apache.spark.SparkContext(conf)
        // sc.setCheckpointDir("checkpoint")
        sc
    }
    
    def initJDBC(fs:FileSystem, cfg:Config) = {
        JDBC(cfg.get("database.driver"),
            cfg.get("database.connString"),
            cfg.get("database.userName"),
            cfg.get("database.password"))
    }
    
    override def main(args: Array[String]) {
        println(java.lang.management.ManagementFactory.getRuntimeMXBean.getName)

        //init
        val cfg = new Config(args.headOption.getOrElse("conf/server.properties"))
        val sc = initSparkContext(cfg.slice("spark."))
        implicit val system = ActorSystem("sbd")
        // val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
        val fs = initFileSystem(sc, cfg.slice("fs."))
        val jdbc = initJDBC(fs,cfg)
        val ml = MLSample(sc,sqlContext)
        val ss = SStream(sc)
        val env = Env(system, cfg, fs, jdbc, ml, sc, sqlContext)
        
        
        //spark related processing sample
        import env.sqlContext.implicits._
        
        val black_list = sc.parallelize(Array("fail", "sad")).
          map(black_word => (black_word, black_word))
    
        //sc.setCheckpointDir("hdfs://master:9000/library/streaming/black_list_filter/")
        val ssc = new StreamingContext(sc, Durations.seconds(30))
    
        val input_word = ssc.socketTextStream("localhost", 9999)
    
        val flattenWord = input_word.flatMap(_.split(" ")).
          map(row => {
            (row, row)
          })
    
        val not_black_word = flattenWord.transform(fw => {
          fw.leftOuterJoin(black_list). // 左连接
            filter(_._2._2.isEmpty). // 将黑名单中的过滤掉
            map(_._1) // 只返回关键字
        })
    
        not_black_word.print // 输出
    
        ssc.start
        ssc.awaitTermination
        sc.stop

        Runtime.getRuntime.addShutdownHook(new Thread {
            override def run() {
                println("Shutdown sequence is started")
                system.shutdown()
                fs.close()
                println("Shutdown sequence is completed")
            }
        })
    }
}

