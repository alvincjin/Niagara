package com.alvin.niagara

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite
/**
 * Created by jinc4 on 6/6/2016.
 */


trait SparkTest extends BeforeAndAfterAll {
  this: Suite =>

  private val master = "local[*]"
  private val appName = this.getClass.getSimpleName

 var sc: SparkContext = _

  //def sc = _sc

  var sqlContext: SQLContext = _

  //def sqlContext = _sqlContext

  val conf: SparkConf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)


  override def beforeAll(): Unit = {
    super.beforeAll()
    sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    sqlContext = new SQLContext(sc)
  }

  override def afterAll(): Unit = {
    if (sc != null) {
      sc.stop()
      sc = null
      sqlContext = null
    }

    super.afterAll()
  }

}