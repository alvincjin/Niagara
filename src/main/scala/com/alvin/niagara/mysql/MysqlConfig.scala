package com.alvin.niagara.mysql

import com.typesafe.config.ConfigFactory

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
