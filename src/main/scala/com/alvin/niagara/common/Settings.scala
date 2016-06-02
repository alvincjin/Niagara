package com.alvin.niagara.common

import com.typesafe.config.ConfigFactory

/**
 * Created by jinc4 on 5/29/2016.
 */

trait Settings {

  val appConfig = ConfigFactory.load()

  val kafkaConfig = appConfig.getConfig("kafka")
  val brokerList = kafkaConfig.getString("brokerList")
  val schemaRegistry = kafkaConfig.getString("schemaRegistry")
  val zookeeperHost = kafkaConfig.getString("zookeeper")
  val topic = kafkaConfig.getString("topic")

  val cassConfig = appConfig.getConfig("cassandra")
  val hostList = cassConfig.getString("hostList")
  val keyspace = cassConfig.getString("keyspace")
  val table = cassConfig.getString("table")

  val inputPath = appConfig.getString("inputPath")
  val outputPath = appConfig.getString("outputPath")
}

