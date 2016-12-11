package com.alvin.niagara.sparkservice

import com.alvin.niagara.cassandra.{CassandraConfig, CassandraSession}
import com.alvin.niagara.common.{Post, Response}
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.querybuilder.QueryBuilder

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import collection.JavaConversions._

/**
  * Created by JINC4 on 6/14/2016.
  */
object CassandraService {

  val session = CassandraSession.createSession()
  val keyspace = CassandraConfig.keyspace
  val table = CassandraConfig.table

  def queryPostById(postid: Long): Future[List[Response]] = {

    val query = QueryBuilder.select("postid", "typeid", "creationdate")
      .from(keyspace, table)
      .where(QueryBuilder.eq("postid", postid))
      .limit(100)

    Future {
      val resultSet: ResultSet = session.execute(query)
      resultSet.map { row =>
        Response(row.getLong("postid"), row.getInt("typeid"), row.getLong("creationdate"))
      }.toList
    }

  }

  def insertPost(post: Post): Future[String] = {

    val query = QueryBuilder.insertInto(keyspace, table)
      .value("postid", post.postid)
      .value("typeid", post.typeid)
      .value("tags", seqAsJavaList(post.tags))
      .value("creationdate", post.creationdate)

    Future {
      session.execute(query)
      "Inserted: " + post.tags.toString()
    }
  }


  def updatePost(postid: Long, tags: List[String]): Future[String] = {

    val epoch = System.currentTimeMillis()

    val query = QueryBuilder.update(keyspace, table)
      .`with`(QueryBuilder.set("tags", seqAsJavaList(tags)))
      .and(QueryBuilder.set("creationdate", epoch))
      .where(QueryBuilder.eq("postid", postid))

    Future {
      session.execute(query)
      "Updated: " + tags.toString()
    }
  }

}
