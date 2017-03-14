package com.alvin.niagara.spark

import com.alvin.niagara.config.Config
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.sql.SparkSession

import java.util.HashMap

/**
  * Created by alvinjin on 2017-03-12.
  */
object YelpProducer extends App with Config {

  val spark = SparkSession
    .builder()
    .master(sparkMaster)
    .appName("Spark Batch App")
    .getOrCreate()


  import spark.implicits._

  val businessPath = yelpInputPath+"yelp_academic_dataset_business.json"
  val reviewPath = yelpInputPath+"yelp_academic_dataset_review.json"
  val tipPath = yelpInputPath+"yelp_academic_dataset_tip.json"
  val userPath = yelpInputPath+"yelp_academic_dataset_user.json"
  val checkinPath = yelpInputPath+"yelp_academic_dataset_checkin.json"

  try {

    val businessDF = spark.read.json(businessPath)
    val reviewDF = spark.read.json(reviewPath)
    val tipDF = spark.read.json(tipPath)
    val userDF = spark.read.json(userPath)
    val checkinDF = spark.read.json(checkinPath)



    println(businessDF.schema.json)//.toString()//printSchema()
    println(reviewDF.schema.json)//.printSchema()
      println( tipDF.schema.json)//printSchema()
      println( userDF.schema.json)//printSchema()
      println( checkinDF.schema.json)//printSchema()


    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    // Send some messages
   /* while(true) {
      (1 to messagesPerSec.toInt).foreach { messageNum =>
        val str = (1 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt(10).toString)
          .mkString(" ")

        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
      }

      Thread.sleep(1000)
    }
  */

  } finally {
    spark.stop()
  }


}
