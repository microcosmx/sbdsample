import akka.actor._
import org.apache.hadoop.fs._
import org.apache.spark._
import org.apache.spark.io._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.storage._
import scala.reflect._
import spray.json._

case class Env(
        system: ActorSystem,
        cfg: Config,
        fs: FileSystem,
        db: JDBC,
        ml: MLSample,
        sc: SparkContext,
        sqlContext: SQLContext)
{

    def mkPath(key: String, ext: String) : String = cfg.get("fs.data") + s"$key$ext"

}
