package com.alvin.niagara.sparkstreaming

import com.alvin.niagara.common.{Post, Settings}
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.streaming._
import kafka.serializer.DefaultDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{State, StateSpec, Time}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext, Seconds}

/**
 * Created by jinc4 on 6/2/2016.
 */
object SparkStreamingConsumerApp extends App with Settings {

  val sparkConf = new SparkConf()
    .setAppName("SparkStreamingConsumerApp")
    .setMaster(sparkMaster)
    .set("spark.cassandra.connection.host", cassHost)
    .set("spark.cassandra.connection.keep_alive_ms", "60000")

  val context = StreamingContext.getOrCreate(checkpointDir, functionToCreateContext _)

  createTables(CassandraConnector(sparkConf))

  val kafkaConf = Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
    "zookeeper.connect" -> zookeeperHost,
    ConsumerConfig.GROUP_ID_CONFIG -> "SparkStreamingConsumer",
    "zookeeper.connection.timeout.ms" -> "1000")

  val messages = KafkaUtils
    .createStream[String, Array[Byte], DefaultDecoder, DefaultDecoder](context, kafkaConf,
    Map(topic -> 1), StorageLevel.MEMORY_AND_DISK)
    .map { case (key, record: Array[Byte]) => Post.deserializeToClass(record) }

  val tagCounts = messages.flatMap(post => post.tags)
    .map { tag => (tag, 1) }

  val updateState = (batchTime: Time, key: String, value: Option[Int], state: State[Int]) => {

    //println(key + " ==== " + state)
    val sum = value.getOrElse(0) + state.getOption.getOrElse(0)
    state.update(sum)
    Some((key, sum))
  }

  val spec = StateSpec.function(updateState)

  // Update the cumulative count using mapWithState()
  // This will give a Dstream made of state (which is the cumulative count of the tags)
  val tagStats = tagCounts.mapWithState(spec)

  tagStats.reduceByKey((a, b)=> Math.max(a, b))
    .filter{case(tag, count)=> count> 30}
    .print()

  messages.saveToCassandra(keyspace, table,
    SomeColumns("postid", "typeid", "tags", "creationdate"))

  context.start()
  context.awaitTermination()


  def functionToCreateContext(): StreamingContext = {

    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint(checkpointDir)
    ssc
  }

  def createTables(connector: CassandraConnector): Unit = {

    connector.withSessionDo { session =>
      session.execute(s"DROP KEYSPACE IF EXISTS $keyspace")
      session.execute(s"CREATE KEYSPACE $keyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute(
        s"""
         CREATE TABLE IF NOT EXISTS $keyspace.$table  (
         postid bigint PRIMARY KEY,
         typeid int,
         tags list<text>,
         creationdate bigint
        )""")
    }
  }


}
