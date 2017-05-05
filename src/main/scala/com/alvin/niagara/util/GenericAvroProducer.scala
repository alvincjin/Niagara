package com.alvin.niagara.util

import java.util.Properties

import com.alvin.niagara.config.Config
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.errors.SerializationException
import org.apache.log4j.Logger

/**
  * Created by alvinjin on 2017-05-05.
  */
object GenericAvroProducer extends App with Config {

  val logger = Logger.getLogger(this.getClass)
  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
  props.put(ProducerConfig.ACKS_CONFIG, "all")
  props.put(ProducerConfig.RETRIES_CONFIG, "0")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    "io.confluent.kafka.serializers.KafkaAvroSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    "io.confluent.kafka.serializers.KafkaAvroSerializer")
  props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081")

  val producer = new KafkaProducer[String, GenericRecord](props)

  val key = "key1"
  val userSchema = "{\"type\":\"record\"," +
    "\"name\":\"myrecord\"," +
    "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}"

  val parser = new Schema.Parser()
  val schema = parser.parse(userSchema)
  val avroRecord = new GenericData.Record(schema)
  avroRecord.put("f1", "value1")

  val record:ProducerRecord[String, GenericRecord] = new ProducerRecord("topic1", key, avroRecord)
  try {
    producer.send(record)

    println("Sent")
  } catch {

      case e :SerializationException => logger.error("Serialization fails "+e)
      case _ => logger.error("Unknown produce failure")

  }

  producer.close()

}
