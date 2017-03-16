package com.alvin.niagara.sparkstreaming

import com.alvin.niagara.config.Config
import com.alvin.niagara.model.{BusinessSerde, PostTags}
import com.datastax.spark.connector.SomeColumns
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
    "group.id" -> "niagara-yelp",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  val topics = Array(businessTopic)


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
    ).map { record => BusinessSerde.deserialize(record.value()) }


    //val ds = messages.flatMap(println(_.toString))
    messages.map(m => println(m.toString)).print()

    /*val tagCounts = messages.flatMap(post => post.tags)
      .map { tag => (tag, 1) }

    val updateState = (batchTime: Time, key: String, value: Option[Int], state: State[Int]) => {
      val sum = value.getOrElse(0) + state.getOption.getOrElse(0)
      state.update(sum)
      Some((key, sum))
    }

    val spec = StateSpec.function(updateState)

    // This will give a Dstream made of state (which is the cumulative count of the tags)
    val tagStats = tagCounts.mapWithState(spec)

    tagStats.reduceByKey((a, b) => Math.max(a, b))
      .filter { case (tag, count) => count > 30 }
      *.print()
*/
  }

}
