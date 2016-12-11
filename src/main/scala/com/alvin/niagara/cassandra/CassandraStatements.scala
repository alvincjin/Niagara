package com.alvin.niagara.cassandra

import com.alvin.niagara.common.Post


/**
  * Created by alvinjin on 2016-12-09.
  */
trait CassandraStatements {

  val config= CassandraConfig

  def createKeyspace =
    s"""
      CREATE KEYSPACE IF NOT EXISTS ${config.keyspace}
    """

  def createTable =
    s"""
       |CREATE TABLE IF NOT EXISTS ${tableName}  (
       |         postid bigint PRIMARY KEY,
       |         typeid int,
       |         tags list<text>,
       |         creationdate bigint
       |         )
   """.stripMargin

  def writeTable(post: Post) =
    s"""
      INSERT INTO ${tableName} (postid, typeid, tags, creationdate)
      VALUES (${post.postid}, ${post.typeid}, ${post.tags}, ${post.creationdate})
    """

  def deleteTable(postid: Long) =
    s"""
      DELETE FROM ${tableName} WHERE postid = ${postid}
    """

  def selectTable(postid: Long) =
    s"""
      SELECT * FROM ${tableName} WHERE postid = ${postid}
    """

  private def tableName = s"${config.keyspace}.${config.table}"
}
