package com.alvin.niagara.sparkstream

import com.alvin.niagara.config.Config
import com.alvin.niagara.model.PostTags
import com.alvin.niagara.util.Util
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

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

  def searchPostById(postid: Long): Future[PostTags] = {

    Future{
      val df = sparkSession.sql(s"SELECT * FROM posts where postid = $postid")
      getPosts(df).head
    }
  }


  def searchPostsByDate(date: String): Future[List[PostTags]] = {

    Future{
      val df = sparkSession.sql(s"SELECT count(*) FROM posts WHERE convertYM(creationdate) = $date")
      getPosts(df).take(5)
    }
  }


  def searchPostsByTag(tag: String): Future[List[PostTags]] = {

    Future {
      val df = sparkSession.sql(s"SELECT * FROM posts")
      val tagDf = df.select("*")
        .where(array_contains(df("tags"), tag))
      getPosts(tagDf).take(5)
    }
  }

  private def getPosts(df: DataFrame): List[PostTags] = {

    import sparkSession.implicits._
    df.map {row => PostTags(row.getAs[Long]("postid"), row.getAs[Int]("typeid"),
      row.getAs[Seq[String]]("tags"), row.getAs[Long]("creationdate"))}
      .collect()
      .toList
  }

  /**
    * Collect all posts contain a specific tag in their tags field
    *
    * @param postDS the given Post dataset
    * @param tag the given tag name
    * @return another Post dataset
    */
  def collectPostsByTag(postDS: Dataset[PostTags], tag: String): Dataset[PostTags] =
    postDS.filter { post: PostTags => post.typeid == 1 && post.tags.contains(tag) }.cache()


  /**
    * Count the number of tags for each month
    *
    * @param postDS the given Post dataset
    * @param sparkSession
    * @return  a RDD contains tuple(month, count)
    */
  def countTagOverMonth(postDS: Dataset[PostTags], sparkSession: SparkSession): RDD[(String, Int)] = {

    import sparkSession.implicits._

    postDS
      .map(post => (Util.getYearMonth(post.creationdate), 1))
      .rdd
      .groupByKey()
      .map { case (month, times) => (month, times.sum) }
      .sortByKey(ascending = true)

  }

  /**
    * Collect all post published in a specific month
    *
    * @param postDS the given Post dataset
    * @param month the given month
    * @return  another Post dataset
    */
  def collectPostsByMonth(postDS: Dataset[PostTags], month: String): Dataset[PostTags] =
    postDS.filter { post:PostTags => Util.getYearMonth(post.creationdate) == month}





  /**
    * Find the month with the most posts
    *
    * @param postDS the given post dataset
    * @param sparkSession
    * @return the (month, count) pair
    */
  def findPopularMonth(postDS: Dataset[PostTags], sparkSession: SparkSession): (String, Int) = {
    import sparkSession.implicits._

    if (postDS.count() == 0)
      ("No posts with this tag", 0)
    else
      postDS
        .map(post => (Util.getYearMonth(post.creationdate), 1))
        .rdd
        .reduceByKey(_ + _)
        .sortBy(_._2)
        .first()
  }


  /**
    * Count the number of tags for each month in streaming
    *
    * @param stream  the given Post streams
    * @param tag the given tag
    */
  def countTagByMonth(stream: DStream[PostTags], tag: String) = {

    stream
      .filter(post => post.typeid == 1 && post.tags.contains(tag))
      .map { post => (Util.getYearMonth(post.creationdate), 1) }
      .reduceByKeyAndWindow((a, b) => a + b, Seconds(60))
      .foreachRDD { rdd => rdd.foreach(println) }
  }


  /**
    * Count the number of posts by each tag
    *
    * @param stream  the given Post streams
    */
  def countPostByTag(stream: DStream[PostTags]) = {

    stream
      .flatMap(post => post.tags)
      .map { tag => (tag, 1) }
      .reduceByKeyAndWindow((a, b) => a + b, Seconds(60))
      .filter{case(tag, count) => count >= 10}
      .foreachRDD { rdd => rdd.foreach(println) }
  }


}
