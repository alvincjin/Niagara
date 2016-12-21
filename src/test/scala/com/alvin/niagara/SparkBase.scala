package com.alvin.niagara

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
 * Created by jinc4 on 6/6/2016.
 */
trait SparkBase {
  private val master = "local[*]"
  private val appName = this.getClass.getSimpleName

  val spark = SparkSession
    .builder()
    .master(master)
    .appName(appName)
    .getOrCreate()


}
