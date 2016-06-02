package com.alvin.niagara.sparkbatch


import java.text.SimpleDateFormat
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.alvin.niagara.common.{Settings, Util, Queries}


/**
 * Created by JINC4 on 5/26/2016.
 */

object SparkBatchSolution extends App with Settings {

  val conf = new SparkConf()
    .setAppName("SparkBatchSolution")
    .setMaster("local[4]")

  val sc = new SparkContext(conf)
  implicit val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  try {

    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")

    val totalPosts = sc.textFile(inputPath)
      .filter(_.contains("<row "))
      .flatMap { line => Util.parseXml(line, sdf) }.toDS()

    val postsOfMonth = Queries.collectPostsByMonth(totalPosts, "2014-07")
    println(s"Total posts in July 2014 : ${postsOfMonth.count()}")

    val stormPosts = Queries.collectPostsByTag(totalPosts, "storm")
    println(s"Total Apache Storm posts: ${stormPosts.count()}")

    val postsByMonth = Queries.collectTagOverMonth(stormPosts)
    postsByMonth.foreach { case (month, times) => println(month + " has Apache Storm posts " + times) }

    val popularMonth = postsByMonth
      .sortBy(_._2, ascending = false)
      .first()

    println(s"Most popular month for Apache Storm posts: ${popularMonth}")

    Util.writeParquet(totalPosts, outputPath)
    println(s"Persisted all posts into parquet files: ${outputPath}")

  } finally {
    sc.stop()
  }


}

