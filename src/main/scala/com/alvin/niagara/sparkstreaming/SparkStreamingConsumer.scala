package com.alvin.niagara.sparkstreaming

import com.alvin.niagara.common.{Queries, Post, Settings}
import com.datastax.spark.connector.cql.CassandraConnector
import kafka.serializer.DefaultDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by jinc4 on 6/1/2016.
 */

object SparkStreamingConsumer extends App with Settings {

  val sparkConf = new SparkConf()
    .setAppName("SparkStreamingConsumer")
    .setMaster("local[*]")
  //.set("spark.cassandra.connection.host", hostList)

  val ssc = new StreamingContext(sparkConf, Seconds(60))

  ssc.checkpoint("./checkpointDir")
  //https://spark.apache.org/docs/1.6.1/streaming-programming-guide.html#initializing-streamingcontext

  // createTables(CassandraConnector(sparkConf))

  val kafkaConf = Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
    "zookeeper.connect" -> zookeeperHost,
    ConsumerConfig.GROUP_ID_CONFIG -> "KafkaConsumer",
    "zookeeper.connection.timeout.ms" -> "1000")

  val topicMaps = Map(topic -> 1)

  val messages = KafkaUtils
    .createStream[String, Array[Byte], DefaultDecoder, DefaultDecoder](ssc, kafkaConf,topicMaps, StorageLevel.MEMORY_ONLY_SER)
    .map { case (key, record: Array[Byte]) => Post.deserializeToClass(record) }

  /*
  messages.foreachRDD { rdd => rdd.foreach(println) }
  Queries.countTagByMonth(messages, "storm")
 */
  /**
   * In each time window, how many posts on the tages
   */


  Queries.countPostByTag(messages)

  //messages.saveToCassandra(keyspace, table,
  //  SomeColumns("postId", "postTypeId", "tags", "creationDate"))

  ssc.start()
  ssc.awaitTermination()


  def createTables(connector: CassandraConnector): Unit = {

    connector.withSessionDo { session =>
      session.execute(s"DROP KEYSPACE $keyspace")
      session.execute(s"CREATE KEYSPACE $keyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute(
        s"""
         CREATE TABLE IF NOT EXISTS $keyspace.$table  (
         postId bigint, postTypeId int, tags list<text>, creationDate bigint
         PRIMARY KEY (postId)
       )""")
    }
  }

}