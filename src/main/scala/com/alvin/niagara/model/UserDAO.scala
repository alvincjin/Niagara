package com.alvin.niagara.model

/**
  * Created by alvinjin on 2016-12-17.
  */


import com.clause.api.persistence.Configs
import slick.driver.MySQLDriver.api._

class UserDAO extends DBManger{


  class Users(tag: Tag) extends Table[User](tag, "users"){


    def id = column[Int]("id", O.SqlType("SERIAL"), O.AutoInc, O.PrimaryKey)
    def userId = column[String]("userId", O.SqlType("CHAR(10)"))
    def username = column[String]("username", O.SqlType("VARCHAR(20)"))
    def password = column[String]("password", O.SqlType("VARCHAR(50)"))
    def email = column[String]("email", O.SqlType("VARCHAR(20)"))
    def verified = column[Boolean]("verified", O.SqlType("BOOLEAN"))

    def * = (id.?, userId, username, password, email, verified) <> (User.tupled, User.unapply)


  }

  lazy val users = TableQuery[Users]


  def createUsersTable(db: Database) = db.run(users.schema.create)

  def InsertUser(db: Database)
}
