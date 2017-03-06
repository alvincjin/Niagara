package com.alvin.niagara.cassandra

import com.alvin.niagara.common.Setting
import com.alvin.niagara.model.RichPost
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import com.datastax.driver.core.{Cluster, QueryOptions, ResultSet}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

/**
  * Created by alvin.jin on 3/2/2017.
  */
trait CassandraDao extends Setting {

  implicit val session = new Cluster.Builder()
    .addContactPoints(hosts.toArray: _*)
    .withPort(port)
    .withQueryOptions(new QueryOptions().setConsistencyLevel(QueryOptions.DEFAULT_CONSISTENCY_LEVEL))
    .build
    .connect


  def execute(query: Select) = Future {
    val resultSet: ResultSet = session.execute(query)
    resultSet.map { row =>
      RichPost(row.getLong("postid"), row.getString("posttype"), row.getString("title"), row.getLong("creationdate"))
    }.toList
  }


  def createTables(keyspace: String, table: String): Unit = {

    session.execute(s"DROP KEYSPACE IF EXISTS $keyspace")
    session.execute(s"CREATE KEYSPACE $keyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute(
      s"""
         | CREATE TABLE IF NOT EXISTS $keyspace.$table (
         |         postid bigint PRIMARY KEY,
         |         posttype text,
         |         title text,
         |         creationdate bigint
         |         )
       """.stripMargin
    )
    session.execute(
      s"""
         |CREATE CUSTOM INDEX post_title_idx ON $keyspace.$table (title)
         |USING 'org.apache.cassandra.index.sasi.SASIIndex'
         |WITH OPTIONS = {
         |'mode': 'CONTAINS',
         |'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer',
         |'case_sensitive': 'false'
         |}
       """.stripMargin)
  }

  def countByType(postType: String): Future[Long] = {

    val query = s"SELECT COUNT(*) as count FROM $keyspace.$table WHERE posttype = '"+postType+"' ALLOW FILTERING"

    Future {
      val resultSet: ResultSet = session.execute(query)
      resultSet.one().getLong("count")
    }
  }


  def queryByTitle(keyword: String): Future[List[RichPost]] = {

    val query = QueryBuilder.select()
      .from(keyspace, table)
      .where(QueryBuilder.like("title", keyword.toUpperCase))
      .limit(10)

    execute(query)
  }


  def queryPostById(postid: Long): Future[List[RichPost]] = {

    val query = QueryBuilder.select()
      .from(keyspace, table)
      .where(QueryBuilder.eq("postid", postid))
      .limit(100)

    execute(query)
  }


  def deletePostById(postId: Long): Future[String] = {

    val query = QueryBuilder.delete()
      .from(keyspace, table)
      .where(QueryBuilder.eq("postid", postId))

    Future {
      session.execute(query)
      s"Deleted ${postId} Successfully"
    }

  }

  def insertPost(post: RichPost): Future[String] = {

    val query = QueryBuilder.insertInto(keyspace, table)
      .value("postid", post.postid)
      .value("posttype", post.posttype)
      .value("title", post.title)
      .value("creationdate", post.creationdate)

    Future {
      session.execute(query)
      s"Inserted ${post.postid} Successfully"
    }
  }


}
