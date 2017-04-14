package com.alvin.niagara.kafkastream

import java.util.{Properties, UUID}
import java.lang.{Double => JDouble, Long => JLong}

import com.alvin.niagara.config.Config
import com.alvin.niagara.model.{Business, Review, User}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.apache.kafka.streams.kstream._

/**
  * Created by alvinjin on 2017-02-06.
  */

object KStreamApp extends App with Config {

  import StreamsConfig._

  val BUZZ_STORE = "business"
  val USER_STORE = "user"
  val CITY_BUZZ_COUNT_STORE = "city-buzz-count"
  val STARS_CITY_STORE = "starts-per-city"

  val settings = new Properties()
  settings.put(APPLICATION_ID_CONFIG, "KstreamApp")
  settings.put(BOOTSTRAP_SERVERS_CONFIG, brokerList)
  settings.put(KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray.getClass.getName)
  settings.put(ConsumerConfig.GROUP_ID_CONFIG, s"${UUID.randomUUID().toString}")
  settings.put(COMMIT_INTERVAL_MS_CONFIG, "10000")
  settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  settings.put(STATE_DIR_CONFIG, stateDir)


  // We want to use `Long` (which refers to `scala.Long`) throughout this code.  However, Kafka
  // ships only with serdes for `java.lang.Long`.  The "trick" below works because there is no
  // `scala.Long` at runtime (in most cases, `scala.Long` is just the primitive `long`), and
  // because Scala converts between `long` and `java.lang.Long` automatically.
  val longSerde: Serde[Long] = Serdes.Long().asInstanceOf[Serde[Long]]
  val stringSerde: Serde[String] = Serdes.String()
  //val longSerde: Serde[JLong] = Serdes.Long()
  val reviewSerde = new ReviewSerde()
  val businessSerde = new BusinessSerde()
  val userSerde = new UserSerde()
  val reviewBusinessUserSerde = new ReviewBusinessUserSerde()


  val builder: KStreamBuilder = new KStreamBuilder
  val reviewStream: KStream[String, Review] = builder.stream(stringSerde, reviewSerde, reviewTopic)
  val businessTable: GlobalKTable[String, Business] = builder.globalTable(stringSerde, businessSerde, businessTopic, BUZZ_STORE)
  val userTable: GlobalKTable[String, User] = builder.globalTable(stringSerde, userSerde, userTopic, USER_STORE)


  import KeyValueImplicits._


  //Join review stream(fact table) with business and user globalktable(dimension tables) on selected join keys, rather than the partition key
  val reviewJoinBusiness: KStream[String, ReviewBusiness] = reviewStream
    .join(businessTable,
      (businessid, review) => review.business_id, //join key
      (r: Review, b: Business) =>
        ReviewBusiness(r.business_id, r.date, r.review_id, r.stars, r.text, r.user_id, b.address,
          b.city, b.latitude, b.longitude, b.name, b.postal_code, b.review_count)
    )


  val reviewJoinBusinessJoinUser: KStream[String, ReviewBusinessUser] = reviewJoinBusiness
    .join(userTable,
      (businessid, reviewBusiness) => reviewBusiness.user_id, //join key
      (r: ReviewBusiness, u: User) =>
        ReviewBusinessUser(r.business_id, r.date, r.review_id, r.stars, r.text, r.user_id, r.address, r.city,
          r.latitude, r.longitude, r.business_name, r.postal_code, r.review_count, u.average_stars, u.fans, u.name, u.yelping_since)
    )


  //reviewJoinBusinessJoinUser.to(stringSerde, reviewBusinessUserSerde, reviewBusinessUserTopic)
  //State Tables for Aggregations

  import KeyValueImplicits._
  import FunctionImplicits.BinaryFunctionToReducer

  val cityStars: KStream[String, Long] = reviewJoinBusinessJoinUser
    .filter((user_id, rbu) => rbu.yelping_since < "2016" && rbu.review_count > 0)
   .map((user_id, rbu) => (rbu.city, rbu.stars))


  val starsPerCity: KTable[String, Long] = cityStars
    .groupBy((city, stars) => city, stringSerde, longSerde)
    .reduce((first: Long, second: Long) => first + second, STARS_CITY_STORE)


  starsPerCity.toStream.print(stringSerde, longSerde)

  val stream: KafkaStreams = new KafkaStreams(builder, settings)

  stream.start()

  //start the Interactive Query Service
  val qs = new QueryService(stream)
  qs.start

}
