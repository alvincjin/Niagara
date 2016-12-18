package com.alvin.niagara.mysql.model

/**
  * Created by alvinjin on 2016-12-17.
  */
import com.github.t3hnar.bcrypt._
import org.mindrot.jbcrypt.BCrypt

case class User(
                 id : Option[Int] = None,
                 userId : String,
                 email : String,
                 hashedPassword : Option[String] = None,
                 name : Option[String] = None,
                 verified : Boolean = false
               ) {
  def withPassword(password: String) = copy (hashedPassword = Some(password.bcrypt(generateSalt)))

  def passwordMatches(password: String): Boolean = hashedPassword.exists(hp => BCrypt.checkpw(password, hp))
}
