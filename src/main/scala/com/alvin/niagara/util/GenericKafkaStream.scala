package com.alvin.niagara.util

import java.util.{Collections, Properties}

import com.alvin.niagara.config.Config
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder}

/**
  * Created by alvinjin on 2017-05-05.
  */
object GenericKafkaStream extends App with Config {


  val streamsConfiguration: Properties = new Properties

  streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-example")
  // Where to find Kafka broker(s).
  streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
  // Where to find the Confluent schema registry instance(s)
  //streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081")
  // Specify default (de)serializers for record keys and for record values.
  //streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
  //streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.getClass.getName)
  streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "2")
  // Records should be flushed every 10 seconds. This is less than the default
  // in order to keep this example interactive.
  //streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000)
  val stringSerde: Serde[String] = Serdes.String

  val schemaRegistryUrl = "http://localhost:8081"
  //val longSerde: Serde[Long] = Serdes.Long
  val schemaRegistry: CachedSchemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100)
  val serdeProps = Collections.singletonMap("schema.registry.url", schemaRegistryUrl)
  // create and configure the SpecificAvroSerdes required in this example
  val playEventSerde: SpecificAvroSerde[MyRecord] = new SpecificAvroSerde(schemaRegistry, serdeProps)
  playEventSerde.configure(serdeProps, false)

  val builder: KStreamBuilder = new KStreamBuilder()
  val playEvents: KStream[String, MyRecord] = builder.stream(Serdes.String, playEventSerde, "topic1")

  playEvents.print(Serdes.String, playEventSerde)

  val stream = new KafkaStreams(builder, streamsConfiguration)
  stream.start()
}
