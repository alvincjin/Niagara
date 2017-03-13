package com.alvin.niagara.config

import com.typesafe.config.ConfigFactory

import scala.collection.JavaConversions._
/**
 * Created by jinc4 on 5/29/2016.
 *
 * The global setting for spark/kafka/cassandra parsed from application.conf
 */

trait Config {

  val appConfig = ConfigFactory.load()

  val sparkConfig = appConfig.getConfig("spark")
  val sparkMaster = sparkConfig.getString("masterUrl")
  val checkpointDir = sparkConfig.getString("checkpointDir")

  val kafkaConfig = appConfig.getConfig("kafka")
  val brokerList = kafkaConfig.getString("brokerList")
  //val schemaRegistry = kafkaConfig.getString("schemaRegistry")
  //val zookeeperHost = kafkaConfig.getString("zookeeper")
  val topic = kafkaConfig.getString("post_topic")
  val textlineTopic = kafkaConfig.getString("textline_topic")
  val uppercaseTopic = kafkaConfig.getString("uppercase_topic")

  val cassandraConfig = appConfig.getConfig("cassandra")
  val hosts: List[String] = cassandraConfig.getStringList("hostList").toList
  val keyspace = cassandraConfig.getString("keyspace")
  val table = cassandraConfig.getString("table")
  val port = cassandraConfig.getInt("port")

  val stackInputPath = appConfig.getString("stackInputPath")
  val outputPath = appConfig.getString("outputPath")
  val yelpInputPath = appConfig.getString("yelpInputPath")
}

