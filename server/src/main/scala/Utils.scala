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
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.streaming._
import scala.collection.JavaConversions._
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import spray.can.Http
import spray.json._

object Utils {
	  
    def makeDF(sc: SparkContext, sqlContext: org.apache.spark.sql.hive.HiveContext, path: String) = {
          println(path)

          val lines = sc.textFile(s"data/$path").map(_.split(",", -1).map(_.trim)).collect
          
          val schema = StructType(lines.head.map(_.split(":").toSeq).map {
              case Seq(name, "string")   => StructField(name, StringType)
              case Seq(name, "integer")  => StructField(name, IntegerType)
              case Seq(name, "double")   => StructField(name, DoubleType)
              case Seq(name, "boolean")  => StructField(name, BooleanType)
          })

          val rows = lines.tail.map(cols => {
              require(cols.size == schema.fields.size)
              Row.fromSeq(cols zip schema map {
                  case ("", StructField(_, _, true, _)) => null
                  case (col, StructField(_, StringType,  _, _)) => col
                  case (col, StructField(_, IntegerType, _, _)) => col.toInt
                  case (col, StructField(_, DoubleType,  _, _)) => col.toDouble
                  case (col, StructField(_, BooleanType, _, _)) => col.toBoolean
              })
          })

          lazy val df = sqlContext.createDataFrame(sc.parallelize(rows), schema)
          df
      }
    
}

