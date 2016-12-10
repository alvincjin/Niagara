package com.alvin.niagara.cassandra

import com.datastax.driver.core._

/**
  * Created by alvinjin on 2016-12-10.
  */
object CassandraSession {

  def createSession(defaultConsistencyLevel: ConsistencyLevel = QueryOptions.DEFAULT_CONSISTENCY_LEVEL) = {
    val cluster = new Cluster.Builder().
      addContactPoints(CassandraConfig.hosts.toArray: _*).
      withPort(CassandraConfig.port).
      withQueryOptions(new QueryOptions().setConsistencyLevel(defaultConsistencyLevel)).build

    val session = cluster.connect

    session
  }

}
