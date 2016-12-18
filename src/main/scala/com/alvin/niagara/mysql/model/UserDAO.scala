package com.alvin.niagara.mysql.model

/**
  * Created by alvinjin on 2016-12-17.
  */


import org.joda.time.{DateTime, DateTimeZone}
import slick.driver.MySQLDriver.api._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Random, Success}

object UserDAO extends DBManager{


  class Users(tag: Tag) extends Table[User](tag,"users") {

    def id              = column[Int] ("id", O.SqlType("SERIAL"), O.AutoInc, O.PrimaryKey)
    def userId          = column[String] ("userId", O.SqlType("CHAR(16)"))
    def email           = column[String] ("email", O.SqlType("VARCHAR(30)"))
    def hashedPassword  = column[Option[String]] ("hashedPassword", O.SqlType("VARCHAR(60)"))
    def name            = column[Option[String]] ("name", O.SqlType("VARCHAR(30)"))
    def verified        = column[Boolean] ("verified", O.SqlType("BOOLEAN"))

    def * = (id.?, userId, email, hashedPassword, name, verified) <> (User.tupled, User.unapply)

    def unique_user_mobile_idx = index("unique_user_email_idx", email, unique = true)
    def unique_user_idx = index("unique_user_id", userId, unique = true)

  }

  lazy val users = TableQuery[Users]

  def dropUsersTable = db.run(users.schema.drop)
  def createUsersTable = db.run(users.schema.create)

  /*
  def InsertUser(user: User): Future[Int] = {

   // val query = users returning users.map(_.id) into ((user, id) => user.copy(id = Some(id)))


    val password = user.password
    //val action = query += user.withPassword(password)
    val action = users += user//.withPassword(password)
    db.run(action)
  }*/

  def createUser(user: User): Future[Option[User]] = {
    val userWithId =
      (users returning users.map(_.id)
        into ((user,id) => user.copy(id=Some(id)))
        ) += user
    db.run(userWithId.asTry).map { result =>
      result match {
        case Success(res) => Some(res)
        case Failure(e: Exception) => None
      }
    }
  }

  def addUser(email: String, name: Option[String], password: Option[String]): Future[Option[User]] = {
    val dt = new DateTime(DateTimeZone.forID("America/New_York"))
    val user = password match {
      case Some(pass) => new User(userId=generateUserId(dt), email=email, name=name).withPassword(pass)
      case _ => new User(userId=generateUserId(dt), email=email, name=name)
    }
    createUser(user)
  }

  def queryByUserId(userId: String): Future[Option[User]] = {
    val action = users.filter(_.userId === userId).result.headOption
    db.run(action)
  }

  def queryUserByEmail(email: String): Future[Option[User]] = {
    val action = users.filter(_.email === email).result.headOption
    db.run(action)
    /*db.run(action.asTry).map { result =>
      result match {
        case Success(res) => res
        case Failure(e: Exception) => None
      }
    }*/
  }

  def generateUserId(dt: DateTime): String = {
    val uId = "U" + Random.alphanumeric.take(2).mkString.toUpperCase + (dt.getMillis()/1000).toString + Random.alphanumeric.take(3).mkString.toUpperCase
    uId
  }


}
