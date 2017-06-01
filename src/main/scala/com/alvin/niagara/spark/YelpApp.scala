package com.alvin.niagara.spark

import com.alvin.niagara.config.Config
import com.alvin.niagara.model.{BusinessSerde, ReviewSerde, UserSerde, _}
import com.alvin.niagara.util.AvroObjectProducer
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._

import scala.util.Random

/**
  * Created by alvinjin on 2017-05-29.
  */
object YelpApp extends App with Config {

  val spark = SparkSession
    .builder()
    .master(sparkMaster)
    .appName("YelpRecommanderApp")
    .getOrCreate()

  import spark.implicits._

  val businessPath = yelpInputPath + "yelp_academic_dataset_business.json"
  val reviewPath = yelpInputPath + "yelp_academic_dataset_review.json"
  val userPath = yelpInputPath + "yelp_academic_dataset_user.json"


  val businessDS: Dataset[Business] = spark.read.json(businessPath).as[Business]
  val reviewDS: Dataset[Review] = spark.read.json(reviewPath).as[Review]
  val userDS: Dataset[User] = spark.read.json(userPath).as[User]


  val businessId2Long = businessDS.map(_.business_id).distinct().rdd.zipWithUniqueId().toDF("business_id", "businessid")


  val userId2Long = userDS.map(_.user_id).distinct().rdd.zipWithUniqueId().toDF("user_id", "userid")

  val reviewsWithUserId = reviewDS.join(userId2Long, "user_id").drop("user_id")//.select("userid", "business_id", "stars")

  val reviewsWithUserBuzzId = reviewsWithUserId .join(businessId2Long, "business_id").drop("business_id")

  reviewsWithUserBuzzId.schema.printTreeString()
/*
  val businessDf = businessDS.toDF().as("buzz_df")
  val reviewDf = reviewDS.toDF().as("rev_df")

  val joinedDF = businessDS.join(reviewDS, "business_id").drop(businessDf("stars")) //businessDS.col("business_id") === reviewDS.col("business_id"))
  //.select("address", "business_id", "categories", "city", "latitude", "longitude",
  //"name", "review_count", "stars", "state", "date", "review_id", "text", "user_id")

  joinedDF.schema.printTreeString()

  //joinedDF.sample(true, 0.1).show(100)
*/
  val trainReviews = reviewsWithUserBuzzId.filter(col("useful")>3 && col("date")> "2007-01-01")
    .select("userid", "businessid", "stars")

  trainReviews.cache()

  trainReviews.show(1000)

  val model = new ALS().
    setSeed(Random.nextLong()).
    setImplicitPrefs(true).
    setRank(10).
    setRegParam(0.01).
    setAlpha(1.0).
    setMaxIter(5).
    setUserCol("userid").
    setItemCol("businessid").
    setRatingCol("stars").
    setPredictionCol("prediction").
    fit(trainReviews)

  trainReviews.unpersist()

  model.userFactors.select("features").show(truncate = false)




  spark.stop()


}
