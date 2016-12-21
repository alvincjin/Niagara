package com.alvin.niagara.mysql


import com.alvin.niagara.mysql.model.UserDAO
import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._

/**
  * Created by alvinjin on 2016-12-18.
  */
object MysqlClient extends App {

  UserDAO.createUsersTable

  UserDAO.insertUser("bog3@gmail.com", "bog3", "1234567")

  val returnUser = UserDAO.queryUsersByEmail("bog3@gmail.com")

  println("Returned User: " + Await.result(returnUser, 10.seconds))


}
