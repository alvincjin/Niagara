package com.alvin.niagara.common

import com.typesafe.config.ConfigFactory

/**
 * Created by jinc4 on 5/29/2016.
 *
 * The global setting for spark/kafka/cassandra parsed from application.conf
 */

trait Setting {

  val appConfig = ConfigFactory.load()

  val sparkConfig = appConfig.getConfig("spark")
  val sparkMaster = sparkConfig.getString("masterUrl")
  val checkpointDir = sparkConfig.getString("checkpointDir")

  val kafkaConfig = appConfig.getConfig("kafka")
  val brokerList = kafkaConfig.getString("brokerList")
  val schemaRegistry = kafkaConfig.getString("schemaRegistry")
  val zookeeperHost = kafkaConfig.getString("zookeeper")
  val topic = kafkaConfig.getString("topic")

  val cassandraConfig = appConfig.getConfig("cassandra")
  val cassHost = cassandraConfig.getString("hostList")
  val keyspace = cassandraConfig.getString("keyspace")
  val table = cassandraConfig.getString("table")

  val inputPath = appConfig.getString("inputPath")
  val outputPath = appConfig.getString("outputPath")
}

