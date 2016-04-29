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

object Main extends App {
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
        val a_rdd = sc.parallelize(Array(1,2,3,4,5))
        println("-----------rdd.count-------------: " + a_rdd.count)
        val nDF = a_rdd.toDF  //.toDF("numberKey")
        nDF.registerTempTable("ntable")
        sqlContext.cacheTable("ntable")
        
        val textFile = sc.textFile("data/attributes.txt")
        println("---------textFile.count---------------: " + textFile.count)
        println(textFile.first)
        val word_count = textFile.flatMap(line => line.split(",")).map(word => (word, 1)).reduceByKey(_+_)
        println(word_count.collect) 
        val tDF = Utils.makeDF(sc, sqlContext, "attributes.txt")
        tDF.registerTempTable("pltable")
        sqlContext.cacheTable("pltable")
        val tDF2 = sqlContext.sql("select * from pltable")
        
        println("---------dataframe---------------: ")
        tDF.show()
        tDF.printSchema()
        tDF.select("pKey").show()
        tDF.select(tDF("pKey"), tDF("val1") + 1).show()
        tDF.filter(tDF("val1") > 21).show()
        tDF.groupBy("pKey").count().show()
        
        //machine learning
        ml.mlexec()
        //ml.mlKMeans()
        
        //streaming
        //ss.streamExec()
        
        //http server actor
        system.actorOf(Props(classOf[Server], env).withRouter(RoundRobinPool(5)), name = "server")
	      implicit val timeout = Timeout(5.seconds) // prevent dead letter when starting
        IO(Http) ! Http.Bind(
            system.actorFor("/user/server"),
            interface = cfg.get("server.host"),
            port = cfg.get("server.port").toInt)

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

