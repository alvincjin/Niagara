package com.alvin.niagara.common


import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SQLContext}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream


/**
 * Created by JINC4 on 5/29/2016.
 */
object Queries {


  def collectPostsByTag(postDS: Dataset[Post], tag: String): Dataset[Post] =
    postDS.filter { post => post.typeid == 1 && post.tags.contains(tag) }.cache()


  def collectTagOverMonth(postDS: Dataset[Post])
                         (implicit sqlContext: SQLContext): RDD[(String, Int)] = {
    import sqlContext.implicits._

    postDS
      .map(post => (getYearMonth(post.creationdate), 1))
      .rdd
      .groupByKey()
      .map { case (month, times) => (month, times.sum) }
      .sortByKey(ascending = true)

  }


  def collectPostsByMonth(postDS: Dataset[Post], month: String): Dataset[Post] =
    postDS.filter(post => getYearMonth(post.creationdate) == month)


  private def getYearMonth(ts: Long): String = new SimpleDateFormat("yyyy-MM").format(new Date(ts))


  def findPopularMonth(postDS: Dataset[Post])
                      (implicit sqlContext: SQLContext): (String, Int) = {
    import sqlContext.implicits._

    if (postDS.count() == 0)
      ("No posts with this tag", 0)
    else
      postDS
        .map(post => (getYearMonth(post.creationdate), 1))
        .rdd
        .reduceByKey(_ + _)
        .sortBy(_._2)
        .first()
  }

  def countTagByMonth(stream: DStream[Post], tag: String) = {

    stream
      .filter(post => post.typeid == 1 && post.tags.contains(tag))
      .map { post => (getYearMonth(post.creationdate), 1) }
      .reduceByKeyAndWindow((a, b) => a + b, Seconds(60))
      .foreachRDD { rdd => rdd.foreach(println) }
  }


  def countPostByTag(stream: DStream[Post]) = {

    stream
      .flatMap(post => post.tags)
      .map { tag => (tag, 1) }
      .reduceByKeyAndWindow((a, b) => a + b, Seconds(60))
      .filter{case(tag, count) => count >= 10}
      .foreachRDD { rdd => rdd.foreach(println) }
  }




}