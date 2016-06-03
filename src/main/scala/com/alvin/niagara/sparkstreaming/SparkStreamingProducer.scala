package com.alvin.niagara.sparkstreaming

import java.text.SimpleDateFormat
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.SparkConf
import com.alvin.niagara.common._

/**
 * Created by JINC4 on 5/29/2016.
 */

/**
 * Spark processed only those files that copied into HDFS after the job execution,
 * it is not read previous files that are in directory.
 */

object SparkStreamingProducer extends App with Settings {

  val conf = new SparkConf()
    .setAppName("SparkStreamingProducer")
    .setMaster(sparkMaster)

  // Create the context
  val ssc = new StreamingContext(conf, Seconds(10))

  val producer = new AvroDefaultEncodeProducer
  val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")

  //Note: The inputPath refers to a hdfs path rather than local linux/windows path
  val postDstream = ssc.textFileStream(inputPath)
    .filter(_.contains("<row "))
    .flatMap { line => Util.parseXml(line, sdf) }
    .map { post => producer.send(post)}

  ssc.start()
  ssc.awaitTermination()

}

