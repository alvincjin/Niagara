package com.alvin.niagara.sparkservice

import com.alvin.niagara.common.{Post, Setting}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import scala.concurrent.Future
import org.apache.spark.sql.functions._
import com.alvin.niagara.common.Util
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created by JINC4 on 6/14/2016.
 *
 * Initialize a Spark context
 * Create a temp Spark table from a Cassandra table
 * by using spark-cassandra connector
 */
object SparkService extends Setting {

  val sparkConf: SparkConf = new SparkConf()
    .setAppName("spark-spray-starter")
    .setMaster(sparkMaster)
    .set("spark.cassandra.connection.host", cassHost)
    .set("spark.cassandra.connection.keep_alive_ms", "60000")

  val sc: SparkContext = new SparkContext(sparkConf)

  val sqlContext: SQLContext = new SQLContext(sc)

  //Register a tmp spark table
  sqlContext.sql( """
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
  sqlContext.udf.register("convertYM", Util.getYearMonth _)

  def searchPostById(postid: Long): Future[Post] = {

    Future{
      val df = sqlContext.sql(s"SELECT * FROM posts where postid = $postid")
      getPosts(df).head
    }
  }


  def searchPostsByDate(date: String): Future[List[Post]] = {

    Future{
      val df = sqlContext.sql(s"SELECT count(*) FROM posts WHERE convertYM(creationdate) = $date")
      getPosts(df).take(5)
    }
  }


  def searchPostsByTag(tag: String): Future[List[Post]] = {

    Future {
      val df = sqlContext.sql(s"SELECT * FROM posts")
      val tagDf = df.select("*")
        .where(array_contains(df("tags"), tag))
      getPosts(tagDf).take(5)
    }
  }

  private def getPosts(df: DataFrame): List[Post] = {

    /*df.map {row => Post(row.getAs[Long]("postid"), row.getAs[Int]("typeid"),
      row.getAs[Seq[String]]("tags"), row.getAs[Long]("creationdate"))}
      .collect()
      .toList
      */
    List[Post]()
  }


}
