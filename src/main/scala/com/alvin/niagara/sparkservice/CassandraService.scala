package com.alvin.niagara.sparkservice

import java.nio.ByteBuffer

import com.alvin.niagara.cassandra.{CassandraConfig, CassandraSession}
import com.alvin.niagara.common.{Post, Response}
import com.datastax.driver.core.{ResultSet, Row}
import com.datastax.driver.core.querybuilder.QueryBuilder
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.collection.JavaConversions._
import org.apache.cassandra.utils.ByteBufferUtil
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
        Response(row.getLong("postid"), row.getInt("typeid"),  row.getLong("creationdate"))
      }.toList
    }
    /*
    Future {
      val resultSet: ResultSet = session.execute(query)

      val row: Row = resultSet.one()
      val columnValue = row.getBytesUnsafe(1)//get[Vector[String]]("subscriptions")
      val columnType = row.getColumnDefinitions.getType(1)
      convert(columnType.deserialize(columnValue))

      resultSet.map { row =>
        Post(row.getLong("postid"), row.getInt("typeid"), row.getList("subscriptions", ByteBuffer.class), row.getLong("creationdate"))
      }.toList
    }
    */

  }

  def queryPostByTag(tag: String): Future[List[Response]] = {

    val query = QueryBuilder.select("postid", "typeid", "creationdate")
      .from(keyspace, table)
      .where(QueryBuilder.contains("tags", tag))
      .limit(100)

    Future {
      val resultSet: ResultSet = session.execute(query)
      resultSet.map { row =>
        Response(row.getLong("postid"), row.getInt("typeid"),  row.getLong("creationdate"))
      }.toList
    }

  }


  def deletePostById(postId: Long): Future[Long] = {

    val query = QueryBuilder.delete()
      .from(keyspace, table)
      .where(QueryBuilder.eq("postid",postId))

    Future {
      session.execute(query)
      postId
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
      post.tags.toString()
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
      tags.toString()
    }
  }


  private def convert(obj: Any): AnyRef = {
    obj match {
      case bb: ByteBuffer => ByteBufferUtil.getArray(bb)
      case list: java.util.List[_] => list.view.map(convert).toList
      case set: java.util.Set[_] => set.view.map(convert).toSet
      case map: java.util.Map[_, _] => map.view.map { case (k, v) => (convert(k), convert(v)) }.toMap
      case other => other.asInstanceOf[AnyRef]
    }
  }

}
