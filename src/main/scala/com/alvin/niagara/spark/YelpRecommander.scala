package com.alvin.niagara.spark

import com.alvin.niagara.config.Config
import com.alvin.niagara.model.{BusinessSerde, ReviewSerde, UserSerde, _}
import com.alvin.niagara.util.AvroObjectProducer
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col

/**
  * Created by alvinjin on 2017-05-29.
  */
object YelpRecommander extends App with Config {

  val spark = SparkSession
    .builder()
    .master(sparkMaster)
    .appName("YelpRecommanderApp")
    .getOrCreate()

  import spark.implicits._

  val businessPath = yelpInputPath + "yelp_academic_dataset_business.json"
  val reviewPath = yelpInputPath + "yelp_academic_dataset_review.json"
  val userPath = yelpInputPath + "yelp_academic_dataset_user.json"

  try {

    val businessDS: Dataset[Business] = spark.read.json(businessPath).as[Business]
    val reviewDS: Dataset[Review] = spark.read.json(reviewPath).as[Review]
    val userDS: Dataset[User] = spark.read.json(userPath).as[User]


 //  val businessDf = businessDS.toDF().as("buzz_df")
   // val reviewDf = reviewDS.toDF().as("rev_df")

   val joinedDF = businessDS.join(reviewDS, "business_id")//businessDS.col("business_id") === reviewDS.col("business_id"))
        .select("address",  "business_id", "categories", "city", "latitude", "longitude",
      "name", "review_count", "state",  "date", "review_id", "text", "user_id")

   joinedDF.schema.printTreeString()

  joinedDF.sample(true, 0.1).show(100)




  } finally {
    spark.stop()
  }


}
