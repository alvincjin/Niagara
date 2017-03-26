package com.alvin.niagara.twitter

/**
  * Created by alvinjin on 2017-03-26.
  */

import com.alvin.niagara.config.Config
import com.danielasfregola.twitter4s.TwitterRestClient
import com.danielasfregola.twitter4s.entities._
import scala.concurrent.ExecutionContext.Implicits.global


object UserTopHashTags extends App with Config {

  def getTopHashtags(tweets: Seq[Tweet], n: Int = 10): Seq[(String, Int)] = {

    val hashtags: Seq[Seq[HashTag]] = tweets.map { tweet =>
      tweet.entities.map(_.hashtags).getOrElse(Seq.empty)
    }

    val hashtagTexts: Seq[String] = hashtags.flatten.map(_.text.toLowerCase)

    val hashtagFrequencies: Map[String, Int] = hashtagTexts.groupBy(identity).mapValues(_.size)

    hashtagFrequencies.toSeq.sortBy { case (entity, frequency) => -frequency }.take(n)
  }


  val consumerToken = ConsumerToken(consumerKey, consumerSecret)
  val accessToken = AccessToken(accessKey, accessSecret)

  val restClient = TwitterRestClient(consumerToken, accessToken)

  val user = "odersky"

  val result = restClient.userTimelineForUser(screen_name = user, count = 200)
    .map { ratedData =>

      val tweets = ratedData.data

      val topHashtags: Seq[((String, Int), Int)] = getTopHashtags(tweets).zipWithIndex

      val rankings = topHashtags.map { case ((entity, frequency), idx) => s"[${idx + 1}] $entity (found $frequency times)"}

      println(s"${user.toUpperCase}'S TOP HASHTAGS:")

      println(rankings.mkString("\n"))
  }
}