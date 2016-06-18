package com.alvin.niagara.sparkservice

import akka.http.scaladsl.model.HttpHeader.ParsingResult
import akka.http.scaladsl.model.{HttpHeader, HttpMethods, StatusCodes}
import akka.http.scaladsl.model.headers.HttpCookie
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.MethodRejection
import akka.stream.ActorMaterializer
import com.alvin.niagara.common.Post
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
}

trait RouteService extends AkkaJSONProtocol {


  val sampleHeader: HttpHeader = (HttpHeader.parse("helloheader", "hellowvalue") match {
    case ParsingResult.Ok(header, _) => Some(header)
    case ParsingResult.Error(_) => None
  }
  ).get

  val sparkRoutes =
    path("postid" / LongNumber) {
      id =>
        get {
          (setCookie(HttpCookie(name = "hello", value = "world")) &
            respondWithHeader(sampleHeader)) {

              complete{ SparkService.searchPostById(id) }
            }
        } ~
        {
            reject(MethodRejection(HttpMethods.GET))
        }
    } ~
      path("createdate" / Segment) { (date: String) =>
        get {
          complete{ SparkService.searchPostsByDate(date) }
        }
      } ~
      path("tag" / Segment) { (tag: String) =>
        get {
          complete{ SparkService.searchPostsByTag(tag) }

        }
      }

}



