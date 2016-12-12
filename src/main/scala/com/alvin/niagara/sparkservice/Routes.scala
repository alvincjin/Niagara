package com.alvin.niagara.sparkservice

import akka.http.scaladsl.model.HttpHeader.ParsingResult
import akka.http.scaladsl.model.{HttpEntity, _}
import akka.http.scaladsl.model.headers.HttpCookie
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.MethodRejection
import akka.stream.ActorMaterializer
import com.alvin.niagara.common.{Post, Response, Tags}
import spray.json.DefaultJsonProtocol
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._

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
}

trait Routes extends AkkaJSONProtocol {


  val route =
    path("postid" / LongNumber) { id =>
      get {
        onSuccess(CassandraService.queryPostById(id)) {
          case result: List[Response] =>
            complete(result)
        }

      } ~
        (post & entity(as[Tags])) { t =>
          onSuccess(CassandraService.updatePost(id, t.tags)) {
            case result: String =>
              complete(HttpEntity(ContentTypes.`application/json`, result))
          }
        } ~ {
        reject(MethodRejection(HttpMethods.GET))
      }
    } ~
      path("createdate" / Segment) { (date: String) =>
        get {
          complete(SparkService.searchPostsByDate(date))
        }
      } ~
      path("tag" / Segment) { (tag: String) =>
        get {
          onSuccess(CassandraService.queryPostByTag(tag)) {
            case result: List[Response] =>
              complete(result)
          }
        }
      } ~
      path("post") {
        (post & entity(as[Post])) { p =>
          onSuccess(CassandraService.insertPost(p)) {
            case result: String =>
              complete(HttpEntity(ContentTypes.`application/json`, result))
          }


        }
      }


}



