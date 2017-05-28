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
  val schemaRegistry = kafkaConfig.getString("schemaRegistry")
  val zookeeperHost = kafkaConfig.getString("zookeeper")
  val stateDir = kafkaConfig.getString("stateDir")

  val topicConfig = kafkaConfig.getConfig("topic")
  val postTopic = topicConfig.getString("post")
  val businessTopic = topicConfig.getString("business")
  val checkinTopic = topicConfig.getString("checkin")
  val reviewTopic = topicConfig.getString("review")
  val tipTopic = topicConfig.getString("tip")
  val userTopic = topicConfig.getString("user")
  val reviewBusinessUserTopic = topicConfig.getString("reviewBusinessUser")
  val employeeTopic = topicConfig.getString("employee")



  val cassandraConfig = appConfig.getConfig("cassandra")
  val hosts: List[String] = cassandraConfig.getStringList("hostList").toList
  val keyspace = cassandraConfig.getString("keyspace")
  val table = cassandraConfig.getString("table")
  val port = cassandraConfig.getInt("port")

  val stackInputPath = appConfig.getString("stackInputPath")
  val outputPath = appConfig.getString("outputPath")
  val yelpInputPath = appConfig.getString("yelpInputPath")

}

