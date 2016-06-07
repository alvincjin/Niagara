package com.alvin.niagara.sparkservice

import com.alvin.niagara.common.{Post, Setting}
import org.apache.spark.sql.SQLContext
import spray.http._
import MediaTypes._
import org.apache.spark.{SparkConf, SparkContext}
import spray.routing.Directive.pimpApply
import spray.routing.HttpService
import scala.util.Try
import org.apache.spark.sql.functions._
import com.alvin.niagara.common.Util

/**
 * Created by JINC4 on 6/4/2016.
 *
 * Spark connects to Cassandra cluster to fetch table as CassandraRDD
 * A bunch of routes call Spark SQL queries on the CassandraRDD
 *
 */
trait SparkService extends HttpService with Setting {

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

  sqlContext.udf.register("convertYM", Util.getYearMonth _)

  val sparkRoutes =

    path("count" / "typeid" / Segment) { (id: String) =>
      get {
        respondWithMediaType(`application/json`) {
          complete {
            val df = sqlContext.sql(s"SELECT count(*) FROM posts where typeid = $id")
            //df.show()
            val result = Try(df.rdd.map(r => r(0).asInstanceOf[Long]).collect).toOption
            result match {
              case Some(data) => s"The number of Posts with typeid $id: " + data(0)
              case None => s"The number of Posts with typeid $id is 0."
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
                case Some(data) => s"The number of Posts in $date: "+ data(0)
                case None => s"The number of Posts in $date is 0."
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
              val results = Try(tagDf.map(r =>
                Post(r.getAs[Long]("postid"), r.getAs[Int]("typeid"), r.getAs[Seq[String]]("tags"), r.getAs[Long]("creationdate"))
              ).count()
              ).toOption
              results match {
                case Some(data) => s"The number of Posts tagged as $tag: "+ data
                case None => s"The number of Posts tagged as $tag is 0."
              }
            }
          }
        }
      }

}

