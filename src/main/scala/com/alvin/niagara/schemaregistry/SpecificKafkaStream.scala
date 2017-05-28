package com.alvin.niagara.schemaregistry

import java.util.{Collections, Properties}

import com.alvin.niagara.Employee
import com.alvin.niagara.config.Config
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

/**
  * Created by alvinjin on 2017-05-27.
  */
object SpecificKafkaStream extends App with Config {


  val streamsConfiguration: Properties = new Properties

  streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-example2")

  streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)

  streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry)
  // Specify default (de)serializers for record keys and for record values.
  //streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
  //streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde)

  streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "2")

  val stringSerde: Serde[String] = Serdes.String

  val schemaRegistryCli: CachedSchemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistry, 100)
  val serdeProps = Collections.singletonMap("schema.registry.url", schemaRegistry)

  // create and configure the SpecificAvroSerdes required in this example
  val employeeSerde: SpecificAvroSerde[Employee] = new SpecificAvroSerde((schemaRegistryCli, schemaRegistry), serdeProps)
  employeeSerde.configure(serdeProps, false)

  val builder: KStreamBuilder = new KStreamBuilder()
  val playEvents: KStream[String, Employee] = builder.stream(Serdes.String, employeeSerde, employeeTopic)

  playEvents.print(Serdes.String, employeeSerde)

  val stream = new KafkaStreams(builder, streamsConfiguration)
  stream.start()
}
