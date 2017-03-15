package com.alvin.niagara.util

import java.util.Properties

import com.alvin.niagara.config.Config
import com.alvin.niagara.model.{Entity, PostTags}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

/**
  * Created by alvinjin on 2017-03-13.
  */
class AvroObjectProducer(topic: String) extends Config {


  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.ByteArraySerializer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.ACKS_CONFIG, "all")

  val producer = new KafkaProducer[String, Array[Byte]](props)


  def send(key: String, value: Array[Byte]) = {

    val record = new ProducerRecord[String, Array[Byte]](topic, null, value)
    producer.send(record)
    println(topic +" "+key)
  }

  def close() = producer.close()
}
