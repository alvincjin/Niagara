package com.alvin.niagara.service

import akka.http.scaladsl.model.HttpHeader.ParsingResult
import akka.http.scaladsl.model.{HttpEntity, _}
import akka.http.scaladsl.model.headers.HttpCookie
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.MethodRejection
import akka.stream.ActorMaterializer
import com.alvin.niagara.common.{Post, Response, Tags}
import spray.json.DefaultJsonProtocol
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import com.alvin.niagara.mysql.model.UserDAO
import com.alvin.niagara.mysql.model.UserDAO.User

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by JINC4 on 6/4/2016.
  *
  * RouteService contains a bunch of directives calling Spark SQL queries
  *
  */

trait AkkaJSONProtocol extends DefaultJsonProtocol {
  implicit val postFormat = jsonFormat4(Post.apply)
  implicit val responseFormat = jsonFormat3(Response.apply)
  implicit val tagFormat = jsonFormat1(Tags.apply)
  implicit val userFormat = jsonFormat6(User.apply)
}

trait Routes extends AkkaJSONProtocol {


  val route =
    path("postid" / LongNumber) { id =>
      //Query posts by specific postid
      get {
        onSuccess(CassandraService.queryPostById(id)) {
          case result: List[Response] =>
            complete(result)
        }
      } ~ delete {
        onSuccess(CassandraService.deletePostById(id)) {
          case result: Long =>
            complete(HttpEntity(ContentTypes.`application/json`, result.toString))
        }
      } ~
        //Update the tag list by looking up the postid
        (post & entity(as[Tags])) { t =>
          onSuccess(CassandraService.updatePost(id, t.tags)) {
            case result: String =>
              complete(HttpEntity(ContentTypes.`application/json`, result))
          }
        } ~ {
        reject(MethodRejection(HttpMethods.GET))
      }
    } ~
      //Query posts containing a specific element in its tags
      path("tag" / Segment) { (tag: String) =>
        get {
          onSuccess(CassandraService.queryPostByTag(tag)) {
            case result: List[Response] =>
              complete(result)
          }
        }
      } ~
      //Create a new post by the payload
      path("post") {
        (post & entity(as[Post])) { p =>
          onSuccess(CassandraService.insertPost(p)) {
            case result: String =>
              complete(HttpEntity(ContentTypes.`application/json`, result))
          }


        }
      }

  val authRoute = path("users" / Segment) { email =>
    get {
      onSuccess(UserDAO.queryUserByEmail(email)) {
        case result: Some[User] =>
          complete(result)
      }

    }
  } ~
    path("user") {
      (post & entity(as[User])) { user =>

        onSuccess(UserDAO.insertUser(user.email, user.username, user.password)) {
          case result: Some[User] =>
            complete(result)
        }
      }
    } ~
    path("user" / Segment) { userid =>
      get {
        onSuccess(UserDAO.queryUserById(userid)) {
          case result: Some[User] =>
            complete(result)
        }
      }
    }


}



