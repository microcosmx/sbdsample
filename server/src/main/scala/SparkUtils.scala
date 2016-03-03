import akka.actor._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage._
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

object SparkUtils {
	def withSparkPool[T](pool: String)(f: => T)(implicit sc: SparkContext) = {
		val oldPool = sc.getLocalProperty("spark.scheduler.pool") // may return null value
		sc.setLocalProperty("spark.scheduler.pool", pool)
		try f
		finally sc.setLocalProperty("spark.scheduler.pool", oldPool)
	}

	def withSparkJob[T](groupId: String, description: String, interruptOnCancel: Boolean = false)(f: => T)(implicit sc: SparkContext) = {
		sc.setJobGroup(groupId, description, interruptOnCancel)
        // we expect blocking Spark calls inside function f
		try blocking { f } 
		finally sc.clearJobGroup()
	}

    def withCachedRDD[T,U](rdd: RDD[T])(f: RDD[T] => U) = {
        rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
        try f(rdd)
        finally rdd.unpersist(blocking = false)        
    }

    def withCachedDF[T](df: DataFrame)(f: DataFrame => T) = {
        df.persist(StorageLevel.MEMORY_AND_DISK_SER)
        try f(df)
        finally df.unpersist(blocking = false)        
    }

    // Simulate broadcast join operation (Spark 1.5.3) with UDF
    implicit def dataFrameExtra(self: DataFrame) = new {

        def filterDimension(that: DataFrame, name: String): DataFrame =
            filterDimension(that.select(name).collect.map(_.getString(0)), name)

        def filterDimension(keys: Seq[String], name: String): DataFrame = {
            if (keys.isEmpty)
                self.filter(lit(false))
            else if (keys.size <= 10)
                self.filter(keys.map(col(name) === _).reduce(_ || _))
            else {
                val keysSet = self.sqlContext.sparkContext.broadcast(keys.toSet)
                val func = udf[Boolean, String](keysSet.value contains _)
                self.filter(func(self(name)))
            }
        }

        def filterDimension(that: DataFrame, name1: String, name2: String): DataFrame =
            filterDimension(that.select(name1, name2).collect.map(row => (row.getString(0), row.getString(1))), name1, name2)

        def filterDimension(keys: Seq[(String, String)], name1: String, name2: String): DataFrame = {
            if (keys.isEmpty)
                self.filter(lit(false))
            else if (keys.size <= 10)
                self.filter(keys.map { case (value1, value2) => col(name1) === value1 && col(name2) === value2 } reduce (_ || _))
            else {
                val keysSet = self.sqlContext.sparkContext.broadcast(keys.toSet)
                val func = udf[Boolean, String, String]((value1, value2) => keysSet.value contains (value1, value2))
                self.filter(func(self(name1), self(name2)))
            }
        }

        def filterProduct(key: String): DataFrame = filterProducts(Seq(key))
        def filterProducts(keys: Seq[String]): DataFrame = filterProducts(keys)
        def filterProducts(that: DataFrame): DataFrame = filterDimension(that, "productKey")

        def filterLocation(key: String): DataFrame = filterLocations(Seq(key))
        def filterLocations(keys: Seq[String]): DataFrame = filterLocations(keys)
        def filterLocations(that: DataFrame): DataFrame = filterDimension(that, "locationKey")

        def filterNodes(that: DataFrame) = filterDimension(that, "nodeKey", "nodeTypeFlag")
    } 
}

trait SparkActor { this: Actor =>
    def sc: SparkContext
    
    def sparkJobId = self.path.toString
    def sparkJobName = "SparkActor"

    def withSparkJob[T](f: => T): T =
    	SparkUtils.withSparkJob(sparkJobId, sparkJobName, true)(f)(sc)

    def forkSparkJob[T](f: => T) = future { blocking { withSparkJob { f } } }

    def cancelSparkJob() {
        sc.cancelJobGroup(sparkJobId)
    }
}
