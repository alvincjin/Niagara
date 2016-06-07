package com.alvin.niagara

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

/**
 * Created by jinc4 on 6/6/2016.
 */
trait SparkBase {
  private val master = "local[*]"
  private val appName = this.getClass.getSimpleName

  val conf: SparkConf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)

  val sc: SparkContext = new SparkContext(conf)

  val sqlContext: SQLContext =new SQLContext(sc)

}
