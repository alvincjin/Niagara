package com.alvin.niagara.model

/**
  * Created by alvinjin on 2016-12-17.
  */
import com.github.t3hnar.bcrypt._
import org.mindrot.jbcrypt.BCrypt

case class User(id: Option[Int] = None, userId: String, username: String, password: String, email: String, verified: Boolean)
{
  def resetPassword(password: String) = copy(password = password.bcrypt(generateSalt))

  def verifyPassword(plainPassword: String): Boolean = password.exists(
    p => BCrypt.checkpw(plainPassword, password)
  )

}
