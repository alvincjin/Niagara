package com.alvin.niagara.service


import akka.http.scaladsl.server.Directives._
import spray.json.DefaultJsonProtocol
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import com.alvin.niagara.cassandra.UserDAO.User
import com.alvin.niagara.cassandra.{CassandraDao, UserDAO}
import com.alvin.niagara.model.RichPost

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by JINC4 on 6/4/2016.
  *
  * RouteService contains a bunch of directives calling Spark SQL queries
  *
  */

trait AkkaJSONProtocol extends DefaultJsonProtocol with CassandraDao {
  implicit val postFormat = jsonFormat4(RichPost.apply)
  implicit val userFormat = jsonFormat6(User.apply)
}

trait Routes extends AkkaJSONProtocol {

  val route =
    path("postid" / LongNumber) { id =>
      get {
        onSuccess(queryPostById(id)) {
          case result: List[RichPost] =>
            complete(result)
        }
      } ~ delete {
        onSuccess(deletePostById(id)) {
          case result: String =>
            complete(result)
        }
      }
    } ~
      path("post") {
        (post & entity(as[RichPost])) { p =>
          onSuccess(insertPost(p)) {
            case result: String =>
              complete(result)
          }
        }
      } ~
      path("title" / Segment) { title =>
        get {
          onSuccess(queryByTitle(title)) {
            case result: List[RichPost] =>
              //complete(HttpEntity(ContentTypes.`application/json`, result))
            complete(result)
          }
        }
      }~
      path("count" / Segment) { typ =>
        get {
          onSuccess(countByType(typ)) {
            case result: Long =>
              complete(result.toString)
          }
        }
      }



  val authRoute = path("users" / Segment) { email =>
    get {
      onSuccess(UserDAO.queryUsersByEmail(email)) {
        case result: Seq[User] =>
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



