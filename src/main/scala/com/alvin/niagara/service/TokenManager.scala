package com.alvin.niagara.service

/**
  * Created by alvinjin on 2016-12-21.
  */


import authentikat.jwt._
import com.alvin.niagara.mysql.model.UserDAO.User
import com.github.nscala_time.time.Imports._
import scala.collection.immutable.HashMap

object TokenManager {

  val header = JwtHeader("HS256")
  val key = "secretKey"

  def createToken(user: User): String = {

    val tokenMap = HashMap("id" -> user.id, "exp" -> (DateTime.now + 1.days).getMillis() / 1000)

    val claimsSet = JwtClaimsSet(tokenMap)

    JsonWebToken(header, claimsSet, key)

  }

  def validateToken(jwt: String): Boolean =  JsonWebToken.validate(jwt, key)

  def parseToken(jwt: String): Option[String] = {

    val claims: Option[Map[String, String]] = jwt match {
      case JsonWebToken(header, claimsSet, signature) =>
        claimsSet.asSimpleMap.toOption
      case x =>
        None
    }

    val id = claims.getOrElse(Map.empty[String, String]).get("id")

    id
  }

}
