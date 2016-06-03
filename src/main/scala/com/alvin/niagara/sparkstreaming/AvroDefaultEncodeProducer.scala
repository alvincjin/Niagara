package com.alvin.niagara.sparkstreaming

import java.util.Properties

import com.alvin.niagara.common.{Post, Settings}
import org.apache.kafka.clients.producer._


/**
 * Created by JINC4 on 5/26/2016.
 */

class AvroDefaultEncodeProducer extends Settings {

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.ByteArraySerializer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.ACKS_CONFIG, "all")

  val producer = new KafkaProducer[String, Array[Byte]](props)
  /**
   * Produce a batch of Avro records to Kafka.
   * Based on new Producer API.  More details:
   * @param post case class to produce
   * @return A Sequence of FutureRecordMetadata instances (based on Java Future)
   */
  def send(post: Post) = {
    val message = new ProducerRecord[String, Array[Byte]](topic, Post.serializeToAvro(post))
    producer.send(message)
    println("Sent post: "+post.postid)

  }


  def close() = producer.close()
}
