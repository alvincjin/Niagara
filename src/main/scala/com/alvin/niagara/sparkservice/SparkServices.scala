package com.alvin.niagara.sparkservice

import java.text.SimpleDateFormat
import java.util.UUID

import akka.actor.{Actor, ActorContext}
import com.alvin.niagara.common.{Post, Settings, Queries}
import org.apache.spark.sql.SQLContext
import spray.http._

import MediaTypes._
import org.apache.spark.{SparkConf, SparkContext}
import spray.http.StatusCodes._
import spray.http._
import spray.routing.Directive.pimpApply
import spray.routing.HttpService
import scala.collection.parallel.mutable
import scala.util.Try
import org.apache.spark.sql.functions._


trait SparkService extends HttpService with Settings {

  val sparkConf: SparkConf = new SparkConf()
    .setAppName("spark-spray-starter")
    .setMaster(sparkMaster)
    .set("spark.cassandra.connection.host", cassHost)
    .set("spark.cassandra.connection.keep_alive_ms", "60000")

  val sc: SparkContext = new SparkContext(sparkConf)

  val sqlContext: SQLContext = new SQLContext(sc)

  sqlContext.sql(
    """CREATE TEMPORARY TABLE posts
      |USING org.apache.spark.sql.cassandra
      |OPTIONS (
      |  table "posts",
      |  keyspace "test",
      |  cluster "Test Cluster",
      |  pushdown "true"
      |)""".stripMargin)

  sqlContext.udf.register("convertYM", Queries.getYearMonth _)

  val sparkRoutes =

    path("count" / "typeid" / Segment) { (id: String) =>
      get {
        respondWithMediaType(`application/json`) {
          complete {


            val df = sqlContext.sql(s"SELECT count(*) FROM posts where typeid = $id")
            //df.show()
            val result = Try(df.rdd.map(r => r(0).asInstanceOf[Long]).collect).toOption
            result match {
              case Some(data) => data(0).toString //HttpResponse(OK, "The total number of questions is: "+ data(0))
              case None => "0" //HttpResponse(InternalServerError, s"Data is not fetched and something went wrong")
            }
          }
        }
      }
    } ~
      path("count" / "createdate" / Segment) { (date: String) =>
        get {
          respondWithMediaType(`application/json`) {
            complete {

              val df = sqlContext.sql(s"SELECT count(*) FROM posts WHERE convertYM(creationdate) = $date")

              val result = Try(df.rdd.map(r => r(0).asInstanceOf[Long]).collect).toOption
              result match {
                case Some(data) => data(0).toString //HttpResponse(OK, "The total number of questions is: "+ data(0))
                case None => "0" //HttpResponse(InternalServerError, s"Data is not fetched and something went wrong")
              }
            }
          }
        }
      } ~
      path("count" / "tag" / Segment) { (tag: String) =>
        get {
          respondWithMediaType(`application/json`) {
            complete {

              val df = sqlContext.sql(s"SELECT * FROM posts")
              val tagDf = df.select("*")
                .where(array_contains(df("tags"), tag))
              //tagDf.show()
              val results = Try(tagDf.map(r =>
                Post(r.getAs[Long]("postid"), r.getAs[Int]("typeid"), r.getAs[Seq[String]]("tags"), r.getAs[Long]("creationdate"))
              ).count()
              ).toOption
              results match {
                case Some(data) => data.toString //HttpResponse(OK, "The total number of questions is: "+ data(0))
                case None => "Not found" //HttpResponse(InternalServerError, s"Data is not fetched and something went wrong")
              }
            }
          }
        }
      }

  //curl -v -X POST http://localhost:8080/entity -H "Content-Type: application/json" -d "{ \"property\" : \"value\" }"


}

class SparkServices extends Actor with SparkService {

  def actorRefFactory: ActorContext = context

  def receive: Actor.Receive = runRoute(sparkRoutes)
}
