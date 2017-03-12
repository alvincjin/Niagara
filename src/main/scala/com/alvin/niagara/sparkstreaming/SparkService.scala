package com.alvin.niagara.sparkstreaming

import com.alvin.niagara.config.Config
import com.alvin.niagara.model.Post
import com.alvin.niagara.util.Util
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Created by JINC4 on 6/14/2016.
 *
 * Initialize a Spark context
 * Create a temp Spark table from a Cassandra table
 * by using spark-cassandra connector
 */
object SparkService extends Config {

  val sparkSession = SparkSession.builder
    .master(sparkMaster)
    .appName("Spark-Service")
    .config("spark.cassandra.connection.host", hosts.toString())
    .config("spark.cassandra.connection.keep_alive_ms", "60000")
    .getOrCreate()

  //Register a tmp spark table
  sparkSession.sql( """
  |CREATE TEMPORARY TABLE posts
  |USING org.apache.spark.sql.cassandra
  |OPTIONS (
  |  table "posts",
  |  keyspace "test",
  |  cluster "Test Cluster",
  |  pushdown "true"
  |)""".stripMargin
  )

  //Register a SparkSQL UDF
  sparkSession.udf.register("convertYM", Util.getYearMonth _)

  def searchPostById(postid: Long): Future[Post] = {

    Future{
      val df = sparkSession.sql(s"SELECT * FROM posts where postid = $postid")
      getPosts(df).head
    }
  }


  def searchPostsByDate(date: String): Future[List[Post]] = {

    Future{
      val df = sparkSession.sql(s"SELECT count(*) FROM posts WHERE convertYM(creationdate) = $date")
      getPosts(df).take(5)
    }
  }


  def searchPostsByTag(tag: String): Future[List[Post]] = {

    Future {
      val df = sparkSession.sql(s"SELECT * FROM posts")
      val tagDf = df.select("*")
        .where(array_contains(df("tags"), tag))
      getPosts(tagDf).take(5)
    }
  }

  private def getPosts(df: DataFrame): List[Post] = {

    import sparkSession.implicits._
    df.map {row => Post(row.getAs[Long]("postid"), row.getAs[Int]("typeid"),
      row.getAs[Seq[String]]("tags"), row.getAs[Long]("creationdate"))}
      .collect()
      .toList
  }


}
