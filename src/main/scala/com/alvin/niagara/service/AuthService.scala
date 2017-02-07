package com.alvin.niagara.service


import org.json4s.Extraction
import org.json4s.JsonAST.JObject
import com.alvin.niagara.cassandra.{CassandraConfig, CassandraSession}
import com.alvin.niagara.common.{Post, Response}
import com.datastax.driver.core.{ResultSet, Row}
import akka.http.scaladsl.model.HttpHeader.ParsingResult
import akka.http.scaladsl.model.{HttpEntity, _}
import akka.http.scaladsl.model.headers.HttpCookie
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{MethodRejection, Route}
import akka.stream.ActorMaterializer
import com.alvin.niagara.common.{Post, Response, Tags}
import spray.json.DefaultJsonProtocol
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.directives.{Credentials, SecurityDirectives}
import akka.http.scaladsl.settings.RoutingSettings._
import akka.http.scaladsl.server._
import Directives._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Created by alvinjin on 2016-12-18.
  */
object AuthService extends SecurityDirectives {


  def myUserPassAuthenticator(credentials: Credentials): Future[Option[String]] =
    credentials match {
      case p @ Credentials.Provided(id) =>
        Future {
          // potentially
          if (p.verify("p4ssw0rd")) Some(id)
          else None
        }
      case _ => Future.successful(None)
    }
  //implicitly[RoutingSettings]
  val route =

      path("basicAsyncAuth") {

        authenticateBasicAsync(realm = "secure site", myUserPassAuthenticator) { user =>
          complete(s"The user is '$user'")
        }
      }




}
