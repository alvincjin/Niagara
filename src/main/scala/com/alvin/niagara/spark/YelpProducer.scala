package com.alvin.niagara.spark

import com.alvin.niagara.config.Config
import org.apache.spark.sql.{Dataset, SparkSession}
import com.alvin.niagara.model._
import com.alvin.niagara.util.AvroObjectProducer

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

    val businessDS: Dataset[Business] = spark.read.json(businessPath).as[Business]
    val reviewDS: Dataset[Review] = spark.read.json(reviewPath).as[Review]
    val tipDS: Dataset[Tip] = spark.read.json(tipPath).as[Tip]
    val userDS: Dataset[User] = spark.read.json(userPath).as[User]
    val checkinDS: Dataset[Checkin] = spark.read.json(checkinPath).as[Checkin]


    val checkinProducer = new AvroObjectProducer(checkinTopic)

    checkinDS.take(100).map{ r =>
      val msg = CheckinSerde.serialize(r)
      val key = r.business_id
      checkinProducer.send(key, msg)

    }
    //without close(), can't send data actually
    checkinProducer.close()

 } finally {
    spark.stop()
  }


}
