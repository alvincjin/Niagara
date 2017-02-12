package com.alvin.niagara.sparkbatch


import java.text.SimpleDateFormat

import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import com.alvin.niagara.common.{Post, Query, Setting, Util}


/**
  * Created by JINC4 on 5/26/2016.
  *
  * A Spark batch app, which streams lines from a xml file.
  * Parses each line to produce a Post object.
  * Runs some queries, then persists post objects in file sys.
  */

object SparkBatchApp extends App with Setting {

  val spark = SparkSession
    .builder()
    .master(sparkMaster)
    .appName("Spark Batch App")
    .getOrCreate()


  import spark.implicits._

  try {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")

    val totalPosts: Dataset[Post] = spark.read.textFile(inputPath)
      .filter{ l:String => l.contains("<row ")}
      .flatMap{ line:String => Util.parseXml(line, sdf) }

    val postsOfMonth = Query.collectPostsByMonth(totalPosts, "2014-07")
    println(s"Total posts in July 2014 : ${postsOfMonth.count()}")

    val stormPosts = Query.collectPostsByTag(totalPosts, "storm")
    println(s"Total Apache Storm posts: ${stormPosts.count()}")

    val postsByMonth = Query.countTagOverMonth(stormPosts, spark)
    postsByMonth.foreach { case (month, times) => println(month + " has Apache Storm posts " + times) }

    val popularMonth = postsByMonth
      .sortBy(_._2, ascending = false)
      .first()

    println(s"Most popular month for Apache Storm posts: ${popularMonth}")

    Util.writeParquet(totalPosts, outputPath)
    println(s"Persisted all posts into parquet files: ${outputPath}")

  } finally {
    spark.stop()
  }


}

