package com.alvin.niagara.sparkservice



import com.alvin.niagara.common.Post
import org.scalatest.{ Matchers, WordSpec }
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.server._
import Directives._
import spray.json.DefaultJsonProtocol
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
//import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created by jinc4 on 6/17/2016.
 */


class RouteServiceSpec extends WordSpec with Matchers
                    with ScalatestRouteTest with RouteService {

  "The Route service" should {

    "return a post object response for GET requests to /postid" in {

      val tags = Seq("java", "cassandra", "storm", "cassandra-jdbc")

      val expect = com.alvin.niagara.common.Post(24698610L, 1, tags, 1405098721353L)

      Get("/postid/24698610") ~> sparkRoutes ~> check {
        responseAs[Post] shouldEqual expect
      }
    }

    "leave GET requests to other paths unhandled" in {
      // tests:
      Get("/id") ~> sparkRoutes ~> check {
        handled shouldBe false
      }
    }

    "return a MethodNotAllowed error for PUT requests to the root path" in {
      // tests:
      Put("/postid/1223333") ~> Route.seal(sparkRoutes) ~> check {
        status === StatusCodes.MethodNotAllowed
        responseAs[String] shouldEqual "HTTP method not allowed, supported methods: GET"
      }
    }
  }
}