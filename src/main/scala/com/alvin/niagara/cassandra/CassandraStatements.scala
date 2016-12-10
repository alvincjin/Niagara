package com.alvin.niagara.cassandra


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

  def writeTable =
    s"""
      INSERT INTO ${tableName} (postid, typeid, tags, creationdate)
      VALUES (?, ?, ?, ?)
    """

  def deleteTable(postid: Long) =
    s"""
      DELETE FROM ${tableName} WHERE postid = ?
    """

  def selectTable =
    s"""
      SELECT * FROM ${tableName} WHERE
        persistence_id = ? AND
        sequence_nr = ?
    """

  private def tableName = s"${config.keyspace}.${config.table}"
}
