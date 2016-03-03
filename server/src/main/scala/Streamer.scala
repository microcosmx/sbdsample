import akka.actor._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success,Failure}
import spray.can._
import spray.http._
import spray.json._

import MediaTypes._

object DataFrameStreamer {

    case object Next
    case object Done
    case object Exit

    def convertColumn(column: StructField, index: Int) : Row => (String, JsValue) = {
        import DefaultJsonProtocol._
        column.dataType match {
            case ByteType      => row:Row => column.name -> row.getByte(index).toJson
            case ShortType     => row:Row => column.name -> row.getShort(index).toJson
            case IntegerType   => row:Row => column.name -> row.getInt(index).toJson
            case LongType      => row:Row => column.name -> row.getLong(index).toJson
            case FloatType     => row:Row => column.name -> row.getFloat(index).toJson
            case DoubleType    => row:Row => column.name -> row.getDouble(index).toJson
            case BooleanType   => row:Row => column.name -> row.getBoolean(index).toJson
            case StringType    => row:Row => column.name -> row.getString(index).toJson
            case BinaryType    => row:Row => column.name -> row.getSeq[Byte](index).toJson
            case DateType      => row:Row => column.name -> row.getDate(index).getTime.toJson
            case TimestampType => row:Row => column.name -> row.getDate(index).getTime.toJson

            case structType: StructType =>
                val convert = fromRow(structType) : Row => JsValue
                row:Row => column.name -> convert(row.getStruct(index))

            case ArrayType(elementType, containsNull) => elementType match {
                case ByteType      => row:Row => column.name -> row.getSeq[Byte](index).toJson
                case ShortType     => row:Row => column.name -> row.getSeq[Short](index).toJson
                case IntegerType   => row:Row => column.name -> row.getSeq[Int](index).toJson
                case LongType      => row:Row => column.name -> row.getSeq[Long](index).toJson
                case FloatType     => row:Row => column.name -> row.getSeq[Float](index).toJson
                case DoubleType    => row:Row => column.name -> row.getSeq[Double](index).toJson
                case StringType    => row:Row => column.name -> row.getSeq[String](index).toJson
                case BinaryType    => row:Row => column.name -> row.getSeq[Seq[Byte]](index).toJson
                case DateType      => row:Row => column.name -> row.getSeq[java.util.Date](index).map(_.getTime).toJson
                case TimestampType => row:Row => column.name -> row.getSeq[java.util.Date](index).map(_.getTime).toJson

                case structType: StructType =>
                    val convert = fromRow(structType) : Row => JsValue
                    row:Row => column.name -> row.getSeq[Row](index).map(convert(_)).toJson
            }

            case MapType(StringType, valueType, valueContainsNull) => valueType match {
                case ByteType      => row:Row => column.name -> JsObject(row.getMap[String, Byte](index).mapValues(_.toJson).toMap)
                case ShortType     => row:Row => column.name -> JsObject(row.getMap[String, Short](index).mapValues(_.toJson).toMap)
                case IntegerType   => row:Row => column.name -> JsObject(row.getMap[String, Int](index).mapValues(_.toJson).toMap)
                case LongType      => row:Row => column.name -> JsObject(row.getMap[String, Long](index).mapValues(_.toJson).toMap)
                case FloatType     => row:Row => column.name -> JsObject(row.getMap[String, Float](index).mapValues(_.toJson).toMap)
                case DoubleType    => row:Row => column.name -> JsObject(row.getMap[String, Double](index).mapValues(_.toJson).toMap)
                case StringType    => row:Row => column.name -> JsObject(row.getMap[String, String](index).mapValues(_.toJson).toMap)
                case BinaryType    => row:Row => column.name -> JsObject(row.getMap[String, Seq[Byte]](index).mapValues(_.toJson).toMap)
                case DateType      => row:Row => column.name -> JsObject(row.getMap[String, java.util.Date](index).mapValues(_.getTime.toJson).toMap)
                case TimestampType => row:Row => column.name -> JsObject(row.getMap[String, java.util.Date](index).mapValues(_.getTime.toJson).toMap)

                case structType: StructType =>
                    val convert = fromRow(structType) : Row => JsValue
                    row:Row => column.name -> JsObject(row.getMap[String, Row](index).mapValues(convert).toMap)
            }
            
        }
    }

    def convertNullableColumn(column: StructField, index: Int) : Row => (String, JsValue) = {
        val f = convertColumn(column, index)
        row:Row => if (row.isNullAt(index)) (column.name -> JsNull) else f(row)
    }

    def fromRow(schema: StructType): Row => JsObject = {
        val convertColumns = schema.fields.zipWithIndex.map {
            case (column, index) if column.nullable => convertNullableColumn(column, index)
            case (column, index) => convertColumn(column, index)
        }
        row:Row => JsObject(convertColumns.map(f => f(row)):_*)
    }

    def toRow(schema: StructType): JsObject => Row = {
        import DefaultJsonProtocol._

        def f(dataType: DataType): JsValue => Any = dataType match {
            case ByteType       => _.convertTo[Byte]
            case ShortType      => _.convertTo[Short]
            case IntegerType    => _.convertTo[Int]
            case LongType       => _.convertTo[Long]
            case FloatType      => _.convertTo[Float]
            case DoubleType     => _.convertTo[Double]
            case BooleanType    => _.convertTo[Boolean]
            case StringType     => _.convertTo[String]
            case BinaryType     => _.convertTo[Seq[Byte]]
            case DateType       => js => new java.util.Date(js.convertTo[Long])
            case TimestampType  => js => new java.util.Date(js.convertTo[Long])

            case structType: StructType => 
                val convert = toRow(structType)
                js => convert(js.asJsObject)

            case ArrayType(elementType, containsNull) =>
                val convert = f(elementType)
                js => js.convertTo[Seq[JsValue]].map {
                    case JsNull if containsNull => null
                    case el => convert(el)
                }

            case MapType(StringType, valueType, valueContainsNull) =>
                val convert = f(valueType)
                _.asJsObject.fields.mapValues {
                    case JsNull if valueContainsNull => null
                    case el => convert(el)
                }.toMap
        }
        val converts = schema.fields.map(_.dataType).map(f)
        js => Row.fromSeq(schema.fields.zip(converts).map {
            case (field, convert) =>
                val value = js.fields.get(field.name).getOrElse(JsNull)
                if (value == JsNull && field.nullable) null
                else convert(value)
        })
    }

    def createIterator(df: DataFrame) = {
        val convert = fromRow(df.schema)
        df.rdd
            .mapPartitions(_.map(convert(_).toString), true)
            .toLocalIterator
    }
}

case class DataFrameStreamer(client: ActorRef, df: DataFrame, drop: Option[Int] = None, take: Option[Int] = None, extraFuture: Future[JsObject] = future { JsObject() }) extends Actor with ActorLogging with SparkActor {
    import DataFrameStreamer._
    def sc = df.sqlContext.sparkContext
    val countPromise = promise[Long]
    var counter = 0L

    override def sparkJobId = s"streamer-${self.path.name}"
    override def sparkJobName = "DataFrameStreamer"

    val iterator = try {
        var iterator = createIterator(df)

        iterator = drop.map(iterator.drop(_)).getOrElse(iterator)
        iterator = take.map(iterator.take(_)).getOrElse(iterator)
        val header = s"""{"schema":${df.schema.json},\n"rows":["""
        
        client ! ChunkedResponseStart(HttpResponse(
            entity = HttpEntity(`application/json`, header))).withAck(Next)
        
        if (!drop.isEmpty || !take.isEmpty)
            countPromise completeWith forkSparkJob { df.count }
        
        iterator.grouped(100).zipWithIndex.map {
            case (rows, n) => (rows.mkString(if (n==0) "" else ",\n", ",\n", ""), rows.size)
        }
    }
    catch {
        case t: Throwable =>
            client ! HttpResponse(
                status = StatusCodes.BadRequest,
                entity = HttpEntity(`text/plain`, t.toString)).withAck(Exit)
            self ! Exit
            Iterator()
    }

    def receive = {
        case Next =>
            forkSparkJob { iterator.hasNext } onComplete {
                case Success(true) =>
                    val (data, count) = iterator.next
                    counter += count
                    client ! MessageChunk(data).withAck(Next)

                case Success(false) =>
                    if (drop.isEmpty && take.isEmpty)
                        countPromise completeWith future { counter }
                    (for (count <- countPromise.future; extra <- extraFuture) yield (count, extra)) onComplete {
                        case Success((count, extra)) =>
                            val data = s"""],\n"count": $count,\n"extra": $extra}"""
                            client ! MessageChunk(data).withAck(Done)
                        case Failure(t) =>
                            client ! MessageChunk(t.toString).withAck(Exit)
                    }
                case Failure(t) =>
                    client ! MessageChunk(t.toString).withAck(Exit)
            }

        case Done =>
            client ! ChunkedMessageEnd.withAck(Exit)

        case Exit =>
            cancelSparkJob()
            context.stop(self)

        case _: Http.ConnectionClosed =>
            cancelSparkJob()
            context.stop(self)
    }
}


case class DataFrameSender(client: ActorRef, df: DataFrame, drop: Option[Int] = None, take: Option[Int] = None, extraFuture: Future[JsObject] = future { JsObject() }) extends Actor with ActorLogging with SparkActor {
    import DataFrameStreamer._
    def sc = df.sqlContext.sparkContext

    override def sparkJobId = s"sender-${self.path.name}"
    override def sparkJobName = "DataFrameSender"

    val header = s"""{"schema": ${df.schema.json},\n"""
    client ! ChunkedResponseStart(HttpResponse(
        entity = HttpEntity(`application/json`, header))).withAck(Next)

    val contentFuture = forkSparkJob {
        var rows = df.collect
        val count = rows.size
        rows = drop.map(rows.drop(_)).getOrElse(rows)
        rows = take.map(rows.take(_)).getOrElse(rows)
        import DefaultJsonProtocol._
        (rows.map(DataFrameStreamer.fromRow(df.schema)).toJson, count)
    }

    def receive = {
        case Next =>
            (for ((rows, count) <- contentFuture; extra <- extraFuture) yield (rows,count,extra)) onComplete {
                case Success((rows,count,extra)) =>
                    client ! MessageChunk(s""""rows": $rows,\n"count": $count,\n"extra": $extra}""").withAck(Done)
                case Failure(t) =>
                    client ! MessageChunk(t.toString).withAck(Exit)
            }

        case Done =>
            client ! ChunkedMessageEnd.withAck(Exit)

        case Exit =>
            cancelSparkJob()
            context.stop(self)

        case _: Http.ConnectionClosed =>
            cancelSparkJob()
            context.stop(self)
    }
}
