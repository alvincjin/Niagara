package com.alvin.niagara.cassandra

import com.typesafe.config.ConfigFactory

/**
  * Created by alvinjin on 2016-12-09.
  */
class CassandraConfig {

  val appConfig = ConfigFactory.load()
  val cassandraConfig = appConfig.getConfig("cassandra")
  val cassHost = cassandraConfig.getString("hostList")
  val keyspace = cassandraConfig.getString("keyspace")
  val table = cassandraConfig.getString("table")

}
