package com.alvin.niagara.mysql

import com.typesafe.config.ConfigFactory
import slick.driver.MySQLDriver.api._
/**
  * Created by alvinjin on 2016-12-17.
  */
object MysqlConfig {

  val appConfig = ConfigFactory.load()
  val mysqlConfig = appConfig.getConfig("mysql")
  val url = mysqlConfig.getString("url")
  val user = mysqlConfig.getString("user")
  val password = mysqlConfig.getString("password")
  val slickDriver = mysqlConfig.getString("slick.driver")
  val dbDriver = mysqlConfig.getString("driver")

}

trait DBManager {

  def db = Database.forURL(
    url = MysqlConfig.url,
    user = MysqlConfig.user,
    password = MysqlConfig.password,
    driver = MysqlConfig.dbDriver
  )

  implicit val session: Session = db.createSession()

}