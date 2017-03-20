package com.alvin.niagara.sparkstreaming

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
    .appName("Spark Batch App")
    .getOrCreate()

  import spark.implicits._

  val businessPath = yelpInputPath + "yelp_academic_dataset_business.json"
  val reviewPath = yelpInputPath + "yelp_academic_dataset_review.json"
  val tipPath = yelpInputPath + "yelp_academic_dataset_tip.json"
  val userPath = yelpInputPath + "yelp_academic_dataset_user.json"
  val checkinPath = yelpInputPath + "yelp_academic_dataset_checkin.json"

  try {

    val businessDS: Dataset[Business] = spark.read.json(businessPath).as[Business]
    val reviewDS: Dataset[Review] = spark.read.json(reviewPath).as[Review]
    val tipDS: Dataset[Tip] = spark.read.json(tipPath).as[Tip]
    val userDS: Dataset[User] = spark.read.json(userPath).as[User]
    val checkinDS: Dataset[Checkin] = spark.read.json(checkinPath).as[Checkin]

/*
    val businessProducer = new AvroObjectProducer(businessTopic)

    businessDS.take(1000).map { r =>
      println("hours: " + r.hours)
      if (r.hours != null && r.categories != null && r.attributes != null) {
        val msg = BusinessSerde.serialize(r)
        val key = r.business_id
        businessProducer.send(key, msg)
      }
    }
    //without close(), can't send data actually
    businessProducer.close()
*/
/*
   val reviewProducer = new AvroObjectProducer(reviewTopic)

    reviewDS.take(1000).map{ r =>
      val msg = ReviewSerde.serialize(r)
      val key = r.business_id
      reviewProducer.send(key, msg)
    }
    //without close(), can't send data actually
    reviewProducer.close()
*//*
     val tipProducer = new AvroObjectProducer(tipTopic)

     tipDS.take(1000).map{ r =>
         val msg = TipSerde.serialize(r)
         val key = r.business_id
         tipProducer.send(key, msg)
     }
     //without close(), can't send data actually
     tipProducer.close()
*/

    val userProducer = new AvroObjectProducer(userTopic)

    userDS.take(1000).map{ r =>
      val msg = UserSerde.serialize(r)
      val key = r.user_id
      userProducer.send(key, msg)
    }
    //without close(), can't send data actually
    userProducer.close()

/*
        val checkinProducer = new AvroObjectProducer(checkinTopic)

        checkinDS.take(1000).map{ r =>
          val msg = CheckinSerde.serialize(r)
          val key = r.business_id
          checkinProducer.send(key, msg)
        }
        //without close(), can't send data actually
        checkinProducer.close()
*/


  } finally {
    spark.stop()
  }


}
