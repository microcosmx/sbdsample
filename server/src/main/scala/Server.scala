import akka.actor._
import akka.event._
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.collection._
import scala.concurrent._
import scala.concurrent.duration._
import scala.reflect._
import scala.reflect.runtime.{universe=>ru}
import scala.sys.process._
import scala.util.matching._
import scala.util.{Success,Failure}
import spray.can.Http
import spray.can.server.Stats
import spray.client.pipelining._
import spray.http._
import spray.http.CacheDirectives._
import spray.httpx.encoding.{Gzip, Deflate}
import spray.httpx.SprayJsonSupport._
import spray.json._
import DefaultJsonProtocol._
import spray.routing._
import spray.routing.directives._
import spray.util._

import CacheDirectives._
import HttpHeaders._
import MediaTypes._


case class Server(env:Env) extends Actor with ActorLogging with HttpService {
    implicit val timeout: Timeout = 1000.seconds
    import context.dispatcher

    def encodeURIComponent(s: String) = 
        java.net.URLEncoder.encode(s, "UTF-8")
            .replace("+", "%20")
            .replace("%21", "!")
            .replace("%27", "'")
            .replace("%28", "(")
            .replace("%29", ")")
            .replace("%7E", "~")

    def actorRefFactory = context

    def receive = runRoute(route)


    def getFromAnyFile(path: String) = dynamic {
        getFromFile(s"src/main/resources/$path") ~
        getFromResource(path)
    }

    def getFromAnyDirectory(path: String) =
        getFromBrowseableDirectory(s"src/main/resources/$path") ~
        getFromResourceDirectory(path)

    val route = {
	      path("ping") {
            complete {
                "PONG!"
            }
        } ~
        pathPrefix("static") {
            getFromAnyDirectory("static")
        } ~
        path("static"/"index") {
            getFromAnyFile("static/index.html")
        } ~
        pathPrefix("dashboard"/"api") {
            path("getIds") {
                parameters('param ? "-1") {
                    (param) =>
                    //val result = env.db.getLocationIdsByDivisionId(Seq(0,1))
                    
                    respondWithMediaType(`application/json`) {
                      //complete { result }
                      complete { "" }
                    }
                }
            } ~
            path("getTables") {
                //mysql database / user table
                val result = env.db.fetch(env.sqlContext, "(select host,user,password from user) as u")
                result.registerTempTable("user")
                env.sqlContext.cacheTable("user")
                sendDataFrame(result)
            }
        } ~
        pathPrefix("admin") {        
            pathPrefix("sql") {
                pathEnd {
                    complete {
                        <html>
                        <body>
                            <h1>SQL</h1>

                            <textarea name="query" form="sql" rows="10" cols="80" title="Enter SQL query here..."></textarea>
                            <br/>
                            <form action={"sql/query"} id="sql" method="POST" target="_blank">
                            <input type="submit" value="Execute SQL Query"/>
                            <input type="hidden" name="stream" value=""/>
                            </form>
                        </body>
                        </html>
                    }
                } ~
                path("query") {
                    anyParam('query) { query =>
                        sendDataFrame(env.sqlContext.sql(query))
                    }
                }
            }
        }
    }
    
    def sendDataFrame(originalDF: DataFrame, extra: Future[JsObject] = future { JsObject() }) =
        anyParam('schema) { _ => complete { originalDF.schema.json.parseJson.asJsObject } } ~
        anyParam('explain) { _ => complete { originalDF.rdd.toDebugString } } ~
        anyParam('filter?,'orderBy?, 'select?, 'distinct?, 'limit.as[Int]?, 'drop.as[Int]?, 'take.as[Int]?, 'count?, 'stream?, 'productGroup?, 'locationGroup?) {
                (filter, orderBy, select, distinct, limit, drop, take, count, stream, productGroup, locationGroup) => ctx =>

            var df = originalDF

            df = filter.map(df.filter(_)).getOrElse(df)
            
            df = select.map(args => df.select(args.split(",").map(df(_)):_*)).getOrElse(df)
            
            df = distinct.map(_ => df.distinct).getOrElse(df)

            df = orderBy.map { args =>
                val cols = args.split(",").map(arg => arg.split(" ") match {
                    case Array(col) => df(col)
                    case Array(col, "asc") => df(col).asc
                    case Array(col, "desc") => df(col).desc
                    case _ => throw new IllegalArgumentException(s"Cannot parse orderBy argument: $arg")
                })
                df.orderBy(cols:_*)
            }.getOrElse(df)

            df = limit.map(df.limit(_)).getOrElse(df)

            if (!count.isEmpty)
                future { ctx.responder ! HttpResponse(entity = HttpEntity(df.count.toString)) }
            else if (!stream.isEmpty)
                (context actorOf Props(DataFrameStreamer(ctx.responder, df, drop, take, extra)))
            else
                (context actorOf Props(DataFrameSender(ctx.responder, df, drop, take, extra)))
        }

    def exceptionHandler = ExceptionHandler {
        case t: Throwable =>
            requestUri { uri =>
                val id = System.currentTimeMillis.toString
                val os = new java.io.ByteArrayOutputStream
                val ps = new java.io.PrintStream(os)
                t.printStackTrace(ps)
                ps.close()
                val stack = os.toString
                log.warning(s"#$id: request to $uri could not be handled normally: $t\n$stack")

                val status = t match {
                    case _: IllegalArgumentException => 400
                    case _: NoSuchElementException => 404
                    case _ => 500
                }
                respondWithStatus(status) {
                    if (uri.path.toString == "/admin/sql/query")
                        complete(t.toString)
                    else
                        complete(s"$status: #$id")
                }
            }
    }
}
