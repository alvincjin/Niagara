package com.alvin.niagara.sparkstream

import java.text.SimpleDateFormat

import com.alvin.niagara.config.Config
import com.alvin.niagara.model.PostTags
import com.alvin.niagara.util.Util
import org.apache.spark.sql.{Dataset, SparkSession}


/**
  * Created by JINC4 on 5/26/2016.
  *
  * A Spark batch app, which streams lines from a xml file.
  * Parses each line to produce a Post object.
  * Runs some queries, then persists post objects in file sys.
  */

object SparkBatchApp extends App with Config {

  val spark = SparkSession
    .builder()
    .master(sparkMaster)
    .appName("Spark Batch App")
    .getOrCreate()


  import spark.implicits._

  try {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")

    val totalPosts: Dataset[PostTags] = spark.read.textFile(stackInputPath)
      .filter{ l:String => l.contains("<row ")}
      .flatMap{ line:String => Util.parseXml(line, sdf) }

    val postsOfMonth = SparkService.collectPostsByMonth(totalPosts, "2014-07")
    println(s"Total posts in July 2014 : ${postsOfMonth.count()}")

    val stormPosts = SparkService.collectPostsByTag(totalPosts, "storm")
    println(s"Total Apache Storm posts: ${stormPosts.count()}")

    val postsByMonth = SparkService.countTagOverMonth(stormPosts, spark)
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

