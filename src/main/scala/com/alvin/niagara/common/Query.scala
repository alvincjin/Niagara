package com.alvin.niagara.common

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SQLContext}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream


/**
 * Created by JINC4 on 5/29/2016.
 *
 * Contains a bunch of queries for aggregations, filtering on Post dataset
 */
object Query {

  /**
   * Collect all posts contain a specific tag in their tags field
   * @param postDS the given Post dataset
   * @param tag the given tag name
   * @return another Post dataset
   */
  def collectPostsByTag(postDS: Dataset[Post], tag: String): Dataset[Post] =
    postDS.filter { post => post.typeid == 1 && post.tags.contains(tag) }.cache()


  /**
   * Count the number of tags for each month
   * @param postDS the given Post dataset
   * @param sqlContext
   * @return  a RDD contains tuple(month, count)
   */
  def countTagOverMonth(postDS: Dataset[Post])
                       (implicit sqlContext: SQLContext): RDD[(String, Int)] = {

    import sqlContext.implicits._

    postDS
      .map(post => (Util.getYearMonth(post.creationdate), 1))
      .rdd
      .groupByKey()
      .map { case (month, times) => (month, times.sum) }
      .sortByKey(ascending = true)

  }

  /**
   * Collect all post published in a specific month
   * @param postDS the given Post dataset
   * @param month the given month
   * @return  another Post dataset
   */
  def collectPostsByMonth(postDS: Dataset[Post], month: String): Dataset[Post] =
    postDS.filter(post => Util.getYearMonth(post.creationdate) == month)





  /**
   * Find the month with the most posts
   * @param postDS the given post dataset
   * @param sqlContext
   * @return the (month, count) pair
   */
  def findPopularMonth(postDS: Dataset[Post])
                      (implicit sqlContext: SQLContext): (String, Int) = {
    import sqlContext.implicits._

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
   * @param stream  the given Post streams
   * @param tag the given tag
   */
  def countTagByMonth(stream: DStream[Post], tag: String) = {

    stream
      .filter(post => post.typeid == 1 && post.tags.contains(tag))
      .map { post => (Util.getYearMonth(post.creationdate), 1) }
      .reduceByKeyAndWindow((a, b) => a + b, Seconds(60))
      .foreachRDD { rdd => rdd.foreach(println) }
  }


  /**
   * Count the number of posts by each tag
   * @param stream  the given Post streams
   */
  def countPostByTag(stream: DStream[Post]) = {

    stream
      .flatMap(post => post.tags)
      .map { tag => (tag, 1) }
      .reduceByKeyAndWindow((a, b) => a + b, Seconds(60))
      .filter{case(tag, count) => count >= 10}
      .foreachRDD { rdd => rdd.foreach(println) }
  }




}