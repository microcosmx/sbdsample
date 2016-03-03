
import java.sql._
import java.lang._
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

case class JDBC(
    driverClass: String,
    connString: String,
    userName: String,
    password: String)
{
    def fetch(sqlContext: SQLContext, queryString: String) = {
        val conf = Map(
            "url" -> connString,
            "dbtable" -> s"($queryString)",
            "user" -> userName,
            "password" -> password)
        val rdd = sqlContext.load("jdbc", conf)
        val schema = rdd.schema
        val schema_ = StructType(schema.map { field => field.dataType match {
                case DateType => field.copy(dataType = StringType)
                case TimestampType => field.copy(dataType = StringType)
                case _ => field
            }
        })
        val dataTypes = schema.map(_.dataType)
        if (schema == schema_) rdd else {
            val rdd_ = rdd.map { row =>
                Row.fromSeq(row.toSeq zip dataTypes map {
                    case (null, _) => null
                    case (value, DateType) => value.toString
                    case (value, TimestampType) => value.toString
                    case (value, _) => value
                })
            }
            sqlContext.createDataFrame(rdd_, schema_)
        }
    }
    
    def fetchJDBC(
      sqlContext: SQLContext,
      queryString: String,
      lowerBound: Long = 0,
      upperBound: Long = Integer.MAX_VALUE,
      partitions: Int = 1) =
    {
      val driver = Class.forName(driverClass).newInstance()
      val conn = DriverManager.getConnection(connString, userName, password)
      var stmt:PreparedStatement  = null
      var result:DataFrame = null
      try{
         stmt = conn.prepareStatement(queryString)
         //log.warn(queryString)
         stmt.setInt(1, 0)
         stmt.setInt(2, -1)
         val rs = stmt.executeQuery()
         val rsm = rs.getMetaData()
         val schema = StructType((1 to rsm.getColumnCount()).map(i => {
         val columnType = rsm.getColumnType(i) match {
             case Types.BOOLEAN   => BooleanType
             case Types.FLOAT     => FloatType
             case Types.DOUBLE    => DoubleType
             case Types.SMALLINT  => IntegerType
             case Types.INTEGER   => IntegerType
             case Types.BIGINT    => LongType
             case Types.DECIMAL   => DoubleType
             case Types.REAL      => FloatType
             case Types.BINARY    => BinaryType
             case Types.VARBINARY => BinaryType
             case Types.DATE      => StringType
             case Types.TIMESTAMP => StringType
             case _               => StringType
         }
         StructField(rsm.getColumnLabel(i).toLowerCase, columnType, rsm.isNullable(i) != 0)
        }))       

         val rows = new JdbcRDD(sqlContext.sparkContext, () => {
           Class.forName(driverClass).newInstance()
           DriverManager.getConnection(connString, userName, password)
         }, queryString, lowerBound, upperBound, partitions).map(cols => Row.fromSeq((schema.fields zip cols).map(x => {
         if (x._2 == null) null
         else x._1.dataType match {
           case StringType => x._2.toString // StringType is used as a fallback for unsupported JDBC types
           case _          => x._2
         }
        })))
        result = sqlContext.createDataFrame(rows, schema)
      }finally {
          if (stmt != null) stmt.close();
          if (conn != null) conn.close();
       }
      result
    }
    
    def execSQL(pSQL: String) = {	    
        val driver = Class.forName(driverClass).newInstance()
        val conn = DriverManager.getConnection(connString, userName, password)
        var isSuccess:Boolean = true
		    var ps: PreparedStatement = null
        try{
		       conn.setAutoCommit(false)
		       ps = conn.prepareStatement(pSQL)
		       ps.executeUpdate()
		       conn.commit()
        }catch {
           case e:SQLException => 
            {
               isSuccess = false
			         if (conn != null) {
                    try {
                       println("Transaction is being rolled back")
                       conn.rollback();
                    }catch {
			                 case e:SQLException => 
                         println("error: " + e + "when rolling back transaction")
                    }   
               }
			     }
        }
        finally {
		        if (ps != null) {
               ps.close();
            }
            conn.setAutoCommit(true);
            conn.close
        }
        isSuccess
    }
    
  def getLocationIdsByDivisionId(divisionIds: Seq[Int]) : Seq[Int] = {
    val dvIds = divisionIds.mkString(",")
    val lQuery = s"""
        select locationid
        from DTOpt.Location l
        where l.divisionid in (${dvIds})
      """
    
    val driver = Class.forName(driverClass).newInstance()
    val conn = DriverManager.getConnection(connString, userName, password)
    var stmt:PreparedStatement = null
    var rs:ResultSet = null
    try{
      stmt = conn.prepareStatement(lQuery)
      rs = stmt.executeQuery()
      val locs:ArrayBuffer[Int] = ArrayBuffer.empty[Int]
      while(rs.next()){
        locs += rs.getInt("locationid")
      }
      locs
    }catch {
      case e:SQLException =>
        println("loc ids error: " + e )
        ArrayBuffer.empty[Int]

    }finally {
      if (null != rs) {
        rs.close();
        rs = null
      }
      if (null != stmt) {
        stmt.close();
        stmt = null
      }
      conn.close
    }
  }

	 def exec(sqlContext: SQLContext, execString: String) = {
        val driver = Class.forName(driverClass).newInstance()
        val conn = DriverManager.getConnection(connString, userName, password)
        var isSuccess:Boolean = true
        try{
           val stmt = conn.prepareStatement(execString)
           stmt.execute()  
        }catch {
           case e:Throwable => 
             println("error: " + e + " sql:" + execString)
             isSuccess = false
        }
        finally {
           conn.close
        }
        isSuccess
    }
}