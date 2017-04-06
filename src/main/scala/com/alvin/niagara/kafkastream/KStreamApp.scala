package com.alvin.niagara.kafkastream

import java.util.{Properties, UUID}

import com.alvin.niagara.config.Config
import com.alvin.niagara.model.{Business, Review, User}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.{GlobalKTable, KStream, KStreamBuilder, KTable}

/**
  * Created by alvinjin on 2017-02-06.
  */

object KStreamApp extends App with Config {

  import StreamsConfig._

  val settings = new Properties()
  settings.put(APPLICATION_ID_CONFIG, "KstreamApp")
  settings.put(BOOTSTRAP_SERVERS_CONFIG, brokerList)
  settings.put(KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray.getClass.getName)
  settings.put(ConsumerConfig.GROUP_ID_CONFIG, s"${UUID.randomUUID().toString}")
  settings.put(COMMIT_INTERVAL_MS_CONFIG, "10000")
  settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  settings.put(STATE_DIR_CONFIG, stateDir)

  val stringSerde: Serde[String] = Serdes.String()
  val reviewSerde = new ReviewSerde()
  val businessSerde = new BusinessSerde()
  val userSerde = new UserSerde()
  val reviewBusinessUserSerde = new ReviewBusinessUserSerde()

  val builder: KStreamBuilder = new KStreamBuilder
  val reviewStream: KStream[String, Review] = builder.stream(stringSerde, reviewSerde, reviewTopic)
  val businessTable: GlobalKTable[String, Business] = builder.globalTable(stringSerde, businessSerde, businessTopic, "business")
  val userTable: GlobalKTable[String, User] = builder.globalTable(stringSerde, userSerde, userTopic, "user")
  import KeyValueImplicits._

  val reviewJoinBusiness: KStream[String, ReviewBusiness] = reviewStream
    // join review stream(fact table) with business global ktable(dimension table) on selected join key, rather than partition key
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

  reviewJoinBusinessJoinUser.print(stringSerde, reviewBusinessUserSerde)
  //reviewJoinBusinessJoinUser.to(stringSerde, reviewBusinessUserSerde, reviewBusinessUserTopic)

  val stream: KafkaStreams = new KafkaStreams(builder, settings)

  stream.start()

}
