
import java.lang._
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark._
import org.apache.spark.streaming._


case class SStream(
    sc: SparkContext)
{
    def streamExec() = {
      val ssc = new StreamingContext(sc, Seconds(1))
      val lines = ssc.socketTextStream("localhost", 9999)
      val words = lines.flatMap(_.split(" "))
      val pairs = words.map(word => (word, 1))
      val wordCounts = pairs.reduceByKey(_ + _)
      // Print the first ten elements of each RDD generated in this DStream to the console
      wordCounts.print()
      ssc.start()             // Start the computation
      ssc.awaitTermination()  // Wait for the computation to terminate
    }
    
}