package com.alvin.niagara.sparkstreaming

import com.alvin.niagara.config.Config
import com.alvin.niagara.model.{PostTags, PostTags$}
import com.alvin.niagara.util.Util
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.streaming._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.{State, StateSpec, Time}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
/**
 * Created by JINC4 on 6/2/2016.
 *
 * A Spark streaming consumer app connects to Kafka
 * Consumes avro messages from Kafka and deserialize them to Post object
 * Runs real-time queries to incrementally update the (tag, count) pairs.
 * Persists post data into Cassandra table
 */
object SparkStreamingConsumer extends App with Config {

  val sparkConf = new SparkConf()
    .setAppName("SparkStreamingConsumerApp")
    .setMaster(sparkMaster)
    .set("spark.cassandra.connection.host", hosts.toString())
    .set("spark.cassandra.connection.keep_alive_ms", "60000")


  val kafkaParams = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[ByteArrayDeserializer],
    ConsumerConfig.GROUP_ID_CONFIG -> "SparkStreamingConsumer",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  Util.createTables(CassandraConnector(sparkConf), keyspace, table)

  val context = StreamingContext.getOrCreate(checkpointDir, functionToCreateContext _)

  context.start()
  context.awaitTermination()


  /**
   * Create a steaming context and setup checkpoint
    *
    * @return
   */
  def functionToCreateContext(): StreamingContext = {

    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint(checkpointDir)

    consumeEventsFromKafka(ssc)
    ssc
  }


  def consumeEventsFromKafka(ssc: StreamingContext) = {

    val messages = KafkaUtils.createDirectStream(
      ssc,
      PreferConsistent,
      Subscribe[String, Array[Byte]](Array(topic), kafkaParams)
    ).map {record => PostTags.deserialize(record.value())}


    val tagCounts = messages.flatMap(post => post.tags)
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
      .print()

    messages.saveToCassandra(keyspace, table,
              SomeColumns("postid", "typeid", "tags","creationdate"))
  }




}
