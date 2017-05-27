package com.alvin.niagara.schemaregistry

import java.util.{Collections, Properties}

import com.alvin.niagara.Employee
import com.alvin.niagara.config.Config
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializerConfig}
import org.apache.kafka.clients.consumer._

import scala.collection.JavaConversions._

/**
  * Created by alvinjin on 2017-05-05.
  */

object ConsumerApp extends App {

  val consumer = new GenericAvroConsumer("group1", "topic5")

  consumer.run()


}

class GenericAvroConsumer(groupId: String, topic: String) extends Config {

  val props = createConsumerConfig(groupId)
  val consumer = new KafkaConsumer[String, Employee](props)


  def createConsumerConfig(groupId: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)

    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer")
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true")
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry)

    props
  }


  def run() = {

    consumer.subscribe(Collections.singletonList(topic))

      while(true) {
        val records: ConsumerRecords[String, Employee] = consumer.poll(100)

        records.map(rec =>
          println("%s %d %d %s \n", rec.topic(), rec.partition, rec.offset(), rec.value())
        )

      }

  }

  def shutdown() = {
    if (consumer != null)
      consumer.close()
  }

}
