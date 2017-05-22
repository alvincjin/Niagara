package com.alvin.niagara.util

import java.util.Properties
import com.alvin.niagara.config.Config
import com.sksamuel.avro4s.AvroSchema
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


object ProducerApp extends App {

  val producer = new GenericAvroProducer("topic3")

  val schema: Schema = AvroSchema[MyRecord]

  val avroRecord = new GenericData.Record(schema)

  avroRecord.put("f1", "value3")

  producer.send("key2", avroRecord)

  producer.close()

}

class GenericAvroProducer(topic: String) extends Config {

  val logger = Logger.getLogger(this.getClass)

  val props = createProducerConfig()
  var producer = new KafkaProducer[String, GenericRecord](props)

  def createProducerConfig(): Properties = {

    val props = new Properties()

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.RETRIES_CONFIG, "0")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry)

    props
  }


  def send(key: String, value: GenericRecord) = {

    val record: ProducerRecord[String, GenericRecord] = new ProducerRecord(topic, key, value)

    try {
      producer.send(record)
    } catch {
      case e: SerializationException => logger.error("Serialization fails " + e)
      case _ => logger.error("Unknown produce failure")

    }

  }


  def close() {
    logger.info("Closing Kafka Producer")
    producer.close()
  }


}
