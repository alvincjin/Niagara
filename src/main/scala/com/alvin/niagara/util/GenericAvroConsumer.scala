package com.alvin.niagara.util

import java.util
import java.util.concurrent.Executors
import java.util.{Collections, Properties}
import collection.JavaConversions._
import com.alvin.niagara.config.Config
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer}
import org.apache.kafka.clients.consumer._
import kafka.utils.VerifiableProperties
import org.apache.avro.generic.GenericRecord

/**
  * Created by alvinjin on 2017-05-05.
  */
object GenericAvroConsumer extends App with Config {

  def createConsumerConfig(brokers: String, groupId: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")

    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer")
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081")

    props
  }




  val topic = "topic1"

  val topicCountMap = new util.HashMap[String, Integer]()
  topicCountMap.put(topic, new Integer(1))

  val props = createConsumerConfig(brokerList, "test3")
  val vProps = new VerifiableProperties(props)
  //val keyDecoder = new KafkaAvroDecoder(vProps)
  //val valueDecoder = new KafkaAvroDecoder(vProps)

  val consumer = new KafkaConsumer[String, GenericRecord](props)

  consumer.subscribe(Collections.singletonList(topic))
  /*
  Executors.newSingleThreadExecutor.execute(new Runnable {
    override def run(): Unit = {
      while (true) {
        val records = consumer.poll(1000)

        for (record: ConsumerRecord[String, Object] <- records)
        System.out.printf("offset = %d, key = %s, value = %s\n",
          record.offset(), record.key(), record.value().toString)
      }
      }

  })
*/
  val records: ConsumerRecords[String, GenericRecord] = consumer.poll(1000)
  for (record:ConsumerRecord[String, GenericRecord] <- records) {
    val avroRecord = record.value()
    val myRecord = MyRecord(avroRecord.get("f1").toString)
    println(myRecord)
  }

  consumer.close()
}
