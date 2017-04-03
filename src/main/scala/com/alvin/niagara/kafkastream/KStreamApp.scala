package com.alvin.niagara.kafkastream

import java.util.{Properties, UUID}

import com.alvin.niagara.config.Config
import com.alvin.niagara.model.{Business, BusinessSerde, Review}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder, KTable}

/**
  * Created by alvinjin on 2017-02-06.
  */

object KStreamApp extends App with Config {

  import StreamsConfig._

  val settings = new Properties()
  settings.put(APPLICATION_ID_CONFIG, "Kstream App")
  settings.put(BOOTSTRAP_SERVERS_CONFIG, brokerList)
  settings.put(KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray.getClass.getName)
  settings.put(ConsumerConfig.GROUP_ID_CONFIG, s"${UUID.randomUUID().toString}")
  settings.put(COMMIT_INTERVAL_MS_CONFIG, "10000")
  settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  settings.put(STATE_DIR_CONFIG, buzzStore)

  val stringSerde: Serde[String] = Serdes.String()
  val reviewSerde = new ReviewSerde()
  val businessSerde = new BusinessSerde()
  val reviewBusinessSerde = new ReviewBusinessSerde()

  val builder: KStreamBuilder = new KStreamBuilder
  val reviewStream: KStream[String, Review] = builder.stream(stringSerde, reviewSerde, reviewTopic)
  val businessTable: KTable[String, Business] = builder.table(stringSerde, businessSerde, businessTopic, buzzStore.split('/').last)

  import KeyValueImplicits._

  val reviewJoinBusiness: KStream[String, ReviewBusiness] = reviewStream
    .join(businessTable, (r: Review, b: Business) =>
          ReviewBusiness(r.business_id, r.date, r.review_id, r.stars, r.text, r.user_id, b.address,
                        b.city, b.latitude, b.longitude, b.name, b.postal_code, b.review_count))

  reviewJoinBusiness.to(stringSerde, reviewBusinessSerde, reviewBusinessTopic)

  val stream: KafkaStreams = new KafkaStreams(builder, settings)

  stream.start()

}
