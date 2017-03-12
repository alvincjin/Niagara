package com.alvin.niagara.dao

/**
  * Created by alvinjin on 2016-12-17.
  */

import com.alvin.niagara.config.DBManager
import com.github.t3hnar.bcrypt._
import org.joda.time.{DateTime, DateTimeZone}
import slick.driver.MySQLDriver.api._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Random, Success}

object UserDAO extends DBManager {

  case class User(
                   id: Option[Int] = None,
                   userid: String,
                   username: String,
                   password: String,
                   email: String,
                   verified: Boolean = false
                 )

  //case class UserPayload(username: String, password: String, email: String)

  class Users(tag: Tag) extends Table[User](tag, "users") {

    def id = column[Int]("id", O.SqlType("SERIAL"), O.AutoInc, O.PrimaryKey)

    def userid = column[String]("userid", O.SqlType("CHAR(16)"))

    def username = column[String]("username", O.SqlType("VARCHAR(30)"))

    def password = column[String]("password", O.SqlType("VARCHAR(60)"))

    def email = column[String]("email", O.SqlType("VARCHAR(30)"))

    def verified = column[Boolean]("verified", O.SqlType("BOOLEAN"))

    def * = (id.?, userid, username, password, email, verified) <>(User.tupled, User.unapply)

    def unique_user_idx = index("unique_user_id", userid, unique = true)

  }

  lazy val users = TableQuery[Users]


  def createUsersTable = db.run(users.schema.create)

  def insertUser(email: String, name: String, password: String): Future[Option[User]] = {

    val user = new User(userid = generateUserId(), username = name,
      password = password.bcrypt(generateSalt), email = email)

    val query = users returning users.map(_.id) into
      ((user, id) => user.copy(id = Some(id)))

    val action = query += user

    db.run(action.asTry).map { result =>
      result match {
        case Success(res) => Some(res)
        case Failure(e: Exception) => None
      }
    }
  }


  def queryUsersByEmail(email: String): Future[Seq[User]] = {
    val action = users.filter(_.email === email)

    db.run(action.result)

  }


  def queryUserById(id: String): Future[Option[User]] = {
    val action = users.filter(_.userid === id).result.headOption
    db.run(action.asTry).map { result =>
      result match {
        case Success(res) => res
        case Failure(e: Exception) => None
      }
    }
  }


  def generateUserId(): String = {

    val dt = new DateTime(DateTimeZone.forID("America/Toronto"))

    Random.alphanumeric.take(5)
      .mkString.toUpperCase + (dt.getMillis() / 1000).toString
  }

}
