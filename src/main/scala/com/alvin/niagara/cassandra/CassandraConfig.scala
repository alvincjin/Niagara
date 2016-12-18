package com.alvin.niagara.cassandra

import com.typesafe.config.ConfigFactory
import collection.JavaConversions._

/**
  * Created by alvinjin on 2016-12-09.
  */

object CassandraConfig
{

    val appConfig = ConfigFactory.load()
    val cassandraConfig = appConfig.getConfig("cassandra")
    val hosts: List[String] = cassandraConfig.getStringList("hostList").toList
    val port = cassandraConfig.getInt("port")
    val keyspace = cassandraConfig.getString("keyspace")
    val table = cassandraConfig.getString("table")

}
