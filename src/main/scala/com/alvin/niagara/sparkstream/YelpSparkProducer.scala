package com.alvin.niagara.sparkstream

import com.alvin.niagara.config.Config
import com.alvin.niagara.model._
import com.alvin.niagara.util.AvroObjectProducer
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by alvinjin on 2017-03-12.
  */
object YelpSparkProducer extends App with Config {

  val spark = SparkSession
    .builder()
    .master(sparkMaster)
    .appName("YelpProducerApp")
    .getOrCreate()

  import spark.implicits._

  val businessPath = yelpInputPath + "yelp_academic_dataset_business.json"
  val reviewPath = yelpInputPath + "yelp_academic_dataset_review.json"
  val userPath = yelpInputPath + "yelp_academic_dataset_user.json"
  val checkinPath = yelpInputPath + "yelp_academic_dataset_checkin.json"
  val tipPath = yelpInputPath + "yelp_academic_dataset_tip.json"

  try {

    val businessDS: Dataset[Business] = spark.read.json(businessPath).as[Business]
    val reviewDS: Dataset[Review] = spark.read.json(reviewPath).as[Review]
    val userDS: Dataset[User] = spark.read.json(userPath).as[User]
    val tipDS: Dataset[Tip] = spark.read.json(tipPath).as[Tip]
    val checkinDS: Dataset[Checkin] = spark.read.json(checkinPath).as[Checkin]


    val businessProducer = new AvroObjectProducer(businessTopic)

    businessDS.collect().map { r =>
      val msg = BusinessSerde.serialize(r)
      val key = r.business_id
      businessProducer.send(key, msg)
    }

    businessProducer.close()


    val reviewProducer = new AvroObjectProducer(reviewTopic)

    reviewDS.take(10000).map { r =>
      val msg = ReviewSerde.serialize(r)
      val key = r.business_id
      reviewProducer.send(key, msg)
    }

    reviewProducer.close()


    val userProducer = new AvroObjectProducer(userTopic)

    userDS.take(10000).map { r =>
      val msg = UserSerde.serialize(r)
      val key = r.user_id
      userProducer.send(key, msg)
    }

    userProducer.close()


    /*
    val checkinProducer = new AvroObjectProducer(checkinTopic)

       checkinDS.take(1000).map{ r =>
          val msg = CheckinSerde.serialize(r)
          val key = r.business_id
          checkinProducer.send(key, msg)
       }
       checkinProducer.close()

       val tipProducer = new AvroObjectProducer(tipTopic)

       tipDS.take(1000).map{ r =>
          val msg = TipSerde.serialize(r)
          val key = r.business_id
          tipProducer.send(key, msg)
       }
       tipProducer.close()
    */


  } finally {
    spark.stop()
  }


}
