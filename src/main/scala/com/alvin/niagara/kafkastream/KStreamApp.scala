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

  /*
  val  jsonSerializer:Serializer[JsonNode] = new JsonSerializer();
  val  jsonDeserializer:Deserializer[JsonNode] = new JsonDeserializer();
  val  jsonSerde:Serde[JsonNode] = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);


  val reviewSerde = Serdes.serdeFrom(ReviewSerde.s, )
*/
  import KeyValueImplicits._

  val reviewSerde = new ReviewSerde()

  val businessSerde = new BusinessSerde()

  // Because this is a KStream ("record stream"), multiple records for the same user will be
  // considered as separate click-count events, each of which will be added to the total count.
  val reviewStream: KStream[String, Review] = builder.stream(stringSerde, reviewSerde, reviewTopic)


  //val businessTable: KTable[String, Business] = builder.table(stringSerde, businessSerde, businessTopic, "BuzzStore")

  val businessStream: KStream[String, Business] = builder.stream(stringSerde, businessSerde, businessTopic)

/*
  val reviewJoinBusiness : KStream[String, (String, Long)] = reviewStream
    // Join the stream against the table.
    //
    // Null values possible: In general, null values are possible for region (i.e. the value of
    // the KTable we are joining against) so we must guard against that (here: by setting the
    // fallback region "UNKNOWN").  In this specific example this is not really needed because
    // we know, based on the test setup, that all users have appropriate region entries at the
    // time we perform the join.
    .leftJoin(businessTable, (clicks: Long, region: String) => (if (region == null) "UNKNOWN" else region, clicks))
*/

  /*
  val textLines: KStream[Array[Byte], String] = builder.stream(postTopic)

  val uppercasedValues: KStream[String, String] = textLines.map((key, value) => (value, value.toUpperCase()))

  uppercasedValues.to(Serdes.String, Serdes.String, postTopic)
*/
  import KeyValueImplicits._

  //val newStream:KStream[String, String] = reviewStream.map((key,value) => (key, value.date))

  val newStream:KStream[String, String] = businessStream.map((key,value) => (key, value.city))

  newStream.to(stringSerde, stringSerde, "2Business")



  val stream: KafkaStreams = new KafkaStreams(builder, settings)

  stream.start()

}
