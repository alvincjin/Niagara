package com.alvin.niagara.sparkml

import com.alvin.niagara.config.Config
import com.alvin.niagara.model._
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._

import scala.util.Random

/**
  * Created by alvinjin on 2017-05-29.
  */
object YelpRecommender extends App with Config {

  val spark = SparkSession
    .builder()
    .master(sparkMaster)
    .appName("YelpRecommanderApp")
    .getOrCreate()

  import spark.implicits._

  val businessPath = yelpInputPath + "yelp_academic_dataset_business.json"
  val reviewPath = yelpInputPath + "yelp_academic_dataset_review.json"
  val userPath = yelpInputPath + "yelp_academic_dataset_user.json"

  val (trainReviews, userInfo, businessInfo )= prepare(businessPath, reviewPath, userPath)

  trainReviews.cache()

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

  //model.userFactors.select("features").show(truncate = false)

  recommend(489396L, model, userInfo, businessInfo)

  spark.stop()


  def prepare(businessPath: String, reviewPath: String, userPath: String): (DataFrame, DataFrame, DataFrame) = {

    val businessDS: Dataset[Business] = spark.read.json(businessPath).as[Business]
    val reviewDS: Dataset[Review] = spark.read.json(reviewPath).as[Review]
    val userDS: Dataset[User] = spark.read.json(userPath).as[User]


    val businessId2Long = businessDS.map(_.business_id).distinct().rdd.zipWithUniqueId().toDF("business_id", "businessid")
    val businessInfo = businessDS.join(businessId2Long, "business_id").select("businessid", "name", "address", "city")


    val userId2Long = userDS.map(_.user_id).distinct().rdd.zipWithUniqueId().toDF("user_id", "userid")
    val userInfo = userDS.join(userId2Long, "user_id").select("userid", "name", "yelping_since")


    val reviewsWithUserId = reviewDS.join(userId2Long, "user_id").drop("user_id")
    val reviewsWithUserBuzzId = reviewsWithUserId.join(businessId2Long, "business_id").drop("business_id")

    val trainReviews = reviewsWithUserBuzzId.filter(col("useful") > 3 && col("date") > "2007-01-01")
      .select("userid", "businessid", "stars")

    (trainReviews, userInfo, businessInfo)

  }

  def recommend(userId: Long, model: ALSModel, users: DataFrame, businesses: DataFrame) = {

    val topRecommendations = makeRecommendations(model, userId, 5)
    val recommendedBuzzIDs =  topRecommendations.select("businessid").as[Long].collect()

    println("=== Businesses recommanded ===")

    users.filter($"userid" === userId).show()
    businesses.filter($"businessid" isin (recommendedBuzzIDs:_*)).show()

  }


  def makeRecommendations(model: ALSModel, userID: Long, howMany: Int): DataFrame = {

    model.itemFactors.printSchema()

    val toRecommend = model.itemFactors.
      select($"id".as("businessid")).
      withColumn("userid", lit(userID))

    model.transform(toRecommend).
      select("businessid", "prediction").
      orderBy($"prediction".desc).
      limit(howMany)
  }

}
