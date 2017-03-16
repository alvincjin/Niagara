package com.alvin.niagara.sparkstreaming

import java.util.UUID

import com.alvin.niagara.config.Config
import com.alvin.niagara.model._
import org.apache.spark._
import org.apache.kafka.common.serialization._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming._

/**
  * Created by alvinjin on 2017-03-15.
  */
object YelpSparkConsumer extends App with Config {

  val conf = new SparkConf()
    .setMaster(sparkMaster)
    .setAppName("YelpConsumer")

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> brokerList,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[ByteArrayDeserializer],
    "group.id" -> s"${UUID.randomUUID().toString}",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  //Completely delete the checkpoint directory for each new message type
  val topics = Array(userTopic)

  val context = StreamingContext.getOrCreate(checkpointDir, functionToCreateContext _)

  context.start()
  context.awaitTermination()


  def functionToCreateContext(): StreamingContext = {

    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint(checkpointDir)

    consumeEventsFromKafka(ssc)
    ssc
  }


  def consumeEventsFromKafka(ssc: StreamingContext) = {

    val messages = KafkaUtils.createDirectStream(
      ssc,
      PreferConsistent,
      Subscribe[String, Array[Byte]](topics, kafkaParams)
    ).map { record => UserSerde.deserialize(record.value()) }


    messages.map(m => println(m.toString)).count().print()

  }

}
