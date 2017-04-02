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

  val builder: KStreamBuilder = new KStreamBuilder

  import StreamsConfig._

  val settings = new Properties()
  settings.put(APPLICATION_ID_CONFIG, "Kstream App")
  settings.put(BOOTSTRAP_SERVERS_CONFIG, brokerList)
  settings.put(KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray.getClass.getName)
  //settings.put(VALUE_SERDE_CLASS_CONFIG, ReviewSerde.getClass.getName)
  settings.put(ConsumerConfig.GROUP_ID_CONFIG, s"${UUID.randomUUID().toString}")
  settings.put(COMMIT_INTERVAL_MS_CONFIG, "10000")
  settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  // Use a temporary directory for storing state, which will be automatically removed after the test.
  settings.put(STATE_DIR_CONFIG, buzzStore)

  val stringSerde: Serde[String] = Serdes.String()
  val reviewSerde = new ReviewSerde()
  val businessSerde = new BusinessSerde()

  val reviewStream: KStream[String, Review] = builder.stream(stringSerde, reviewSerde, reviewTopic)

  val businessTable: KTable[String, Business] = builder.table(stringSerde, businessSerde, businessTopic, "BuzzStore")

  //val businessStream: KStream[String, Business] = builder.stream(stringSerde, businessSerde, businessTopic)

  import KeyValueImplicits._
  val reviewJoinBusiness:KStream[String, String] = reviewStream.join(businessTable, (review: Review, buzz: Business) => buzz.city+" : "+review.text)

  //val newStream:KStream[String, String] = reviewStream.map((key,value) => (key, value.date))
  //val newStream:KStream[String, String] = businessStream.map((key,value) => (key, value.city))
  //val newStream:KStream[String, String] = businessTable.toStream.map((key,value) => (key, value.city))

  reviewJoinBusiness.to(stringSerde, stringSerde, "reviewBusiness")


  val stream: KafkaStreams = new KafkaStreams(builder, settings)

  stream.start()

}
