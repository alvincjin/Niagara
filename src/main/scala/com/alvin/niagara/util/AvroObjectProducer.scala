package com.alvin.niagara.util

import java.util.Properties

import com.alvin.niagara.config.Config
import .Entity
import com.alvin.niagara.model.PostTags
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

/**
  * Created by alvinjin on 2017-03-13.
  */
class AvroObjectProducer(record: Entity) extends Config {


  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.ByteArraySerializer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.ACKS_CONFIG, "all")

  val producer = new KafkaProducer[String, Array[Byte]](props)

  /**
    * Sent a Post object as Avro records to Kafka.
    *
    * @param topic
    * @return A sequence of FutureRecordMetadata instances
    */
  def send(topic: String) = {
    //val message = new ProducerRecord[String, Array[Byte]](topic, record)
    //producer.send(message)
  }

  def close() = producer.close()
}
