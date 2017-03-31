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

      //println(tweet.text.toString)
      tweet.entities.map(_.hashtags).getOrElse(Seq.empty)
    }

    //"@TryCaviar" or "caviar"
    //tweets.filter(_.text.contains("@TryCaviar")).map(x => println(x.id + "  "+x.created_at.toString  +" : " +x.text))
    tweets.map { tweet => println(tweet.id + "  " + tweet.created_at.toString + " : " + tweet.text)
               tweet.entities.map(_.hashtags).getOrElse(Seq.empty).map(_.text.toLowerCase).foreach(println)
      println("#############")
      tweet.entities.map(_.user_mentions).getOrElse(Seq.empty).map(_.screen_name.toLowerCase).foreach(println)
    }

    val hashtagTexts: Seq[String] = hashtags.flatten.map(_.text.toLowerCase)

    hashtagTexts.foreach(println)

    val hashtagFrequencies: Map[String, Int] = hashtagTexts.groupBy(identity).mapValues(_.size)

    hashtagFrequencies.toSeq.sortBy { case (entity, frequency) => -frequency }.take(n)
  }


  val consumerToken = ConsumerToken(consumerKey, consumerSecret)
  val accessToken = AccessToken(accessKey, accessSecret)

  val restClient = TwitterRestClient(consumerToken, accessToken)

  val user = "realDonaldTrump"//"KimKardashian"

  val since_id = None//Some(814147150330228736L)

  val result = restClient.userTimelineForUser(screen_name = user, since_id = since_id, count = 1000)
    .map { ratedData =>

      val tweets = ratedData.data

      val topHashtags: Seq[((String, Int), Int)] = getTopHashtags(tweets).zipWithIndex

      val rankings = topHashtags.map { case ((entity, frequency), idx) => s"[${idx + 1}] $entity (found $frequency times)"}

      println(s"${user.toUpperCase}'S TOP HASHTAGS:")

      println(rankings.mkString("\n"))
  }
}