package com.alvin.niagara.mysql


import com.alvin.niagara.mysql.model.UserDAO

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Promise}
import scala.util.{Failure, Success}
import scala.concurrent.duration._

/**
  * Created by alvinjin on 2016-12-18.
  */
object MysqlClient extends App {

  //UserDAO.dropUsersTable
  UserDAO.createUsersTable

  UserDAO.addUser("bog@gmail.com", Some("bog"), Some("1234567"))

  val returnUser = UserDAO.queryUserByEmail("bog@gmail.com")

 // val returnUser = UserDAO.queryByEmail("bob@gmail.com")
  println("Returned User: " + Await.result(returnUser, 10.seconds))


}
