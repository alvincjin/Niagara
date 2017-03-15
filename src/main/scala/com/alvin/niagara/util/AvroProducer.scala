package com.alvin.niagara.util

import java.util.Properties

import com.alvin.niagara.config.Config
import com.alvin.niagara.model.PostTags
import org.apache.kafka.clients.producer._

/**
 * Created by JINC4 on 5/26/2016.
 *
 * Avro message producer connects to kafka cluster
 * Then, Sents avro message to kafka
 */

class AvroProducer extends Config {

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
    * @param post a case class to send
   * @return A sequence of FutureRecordMetadata instances
   */
  def send(post: PostTags) = {
    val message = new ProducerRecord[String, Array[Byte]](postTopic, PostTags.serialize(post))
    producer.send(message)
  }

  def close() = producer.close()
}
