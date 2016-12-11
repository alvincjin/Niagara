package com.alvin.niagara.sparkservice

import com.alvin.niagara.cassandra.{CassandraSession, CassandraStatements}
import com.alvin.niagara.common.{Post, Response}
import com.datastax.driver.core.ResultSet
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import collection.JavaConversions._

/**
  * Created by JINC4 on 6/14/2016.
  */
object CassandraService extends CassandraStatements {


  val session = CassandraSession.createSession()

  def searchPostById(postid: Long): Future[List[Response]] = {

    val query = selectTable(postid)

    Future {
      val resultSet: ResultSet = session.execute(query)
      resultSet.map { row =>
        Response(row.getLong("postid"), row.getInt("typeid"), row.getLong("creationdate"))
      }.toList
    }

  }

  def insertNewPost(post: Post): Future[String] = {

    val query = writeTable(post)

    Future {
      session.execute(query)
      "Inserted"
    }
  }


}
