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

class SparkTestcase extends FlatSpec with Matchers with BeforeAndAfterAll with TestKitBase {

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

            println(sc.parallelize(1 to 1000).count)
            
            /*
            import scalawebsocket._
            WebSocket().open("ws://localhost:8600/server/?module=cup_info").sendText("text").close().shutdown()
            WebSocket().open("ws://127.0.0.1:8600/server/?module=cup_info").onTextMessage(msg => 
              println(msg)
            )
            * */
            
            
            
            import java.io._
            import org.apache.commons._
            import org.apache.http._
            import org.apache.http.client._
            import org.apache.http.client.methods._
            import org.apache.http.impl.client.DefaultHttpClient
            import java.util.ArrayList
            import org.apache.http.message.BasicNameValuePair
            import org.apache.http.client.entity.UrlEncodedFormEntity
            import org.apache.http.util._
            
            val url = "http://10.131.252.157:8600/server/?module=load_avg";
        
            val client = new DefaultHttpClient
            //val params = client.getParams
            //params.setParameter("module", "cup_info")
            
            val get = new HttpGet(url)
            //val post = new HttpPost(url)
            //post.addHeader("appid","YahooDemo")
            //val nameValuePairs = new ArrayList[NameValuePair](1)
            //nameValuePairs.add(new BasicNameValuePair("registrationid", "123456789"));
            //post.setEntity(new UrlEncodedFormEntity(nameValuePairs));
            
            // send the post request
            //val response = client.execute(post)
            
            //send the get
            val response = client.execute(get)
            println("--- HEADERS ---")
            response.getAllHeaders.foreach(arg => println(arg))
            println("--- Contents ---")
            val entity = response.getEntity()
            println(EntityUtils.toString(entity))
            /*
            byte[] buffer = new byte[1024];
            InputStream inputStream = entity.getContent();
            try {
              int bytesRead = 0;
              BufferedInputStream bis = new BufferedInputStream(inputStream);
              while ((bytesRead = bis.read(buffer)) != -1) {
                String chunk = new String(buffer, 0, bytesRead);
                System.out.println(chunk);
              }
            } catch (IOException ioException) {
              ioException.printStackTrace()
            }
            */
            
        } catch {
              case t: Throwable =>
                  t.printStackTrace()
          }
    }


}
