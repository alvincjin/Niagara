package com.alvin.niagara.sparkservice

/*
import org.specs2.mutable.Specification
import spray.testkit.RouteTest


/**
 * Created by JINC4 on 6/7/2016.
 */

class SparkServiceSpec extends Specification
with Specs2RouteTest with SparkService {

  def actorRefFactory = system

  sequential
  "The service" should {
    "retrieve the number of posts with specific typid" in {
      Get("/count/typeid/1") ~> sparkRoutes ~> check {
        responseAs[String] must contain("The number of Posts with typeid")
      }
    }

    "be able to retrieve the number of posts in a specific month" in {
      Get("/count/createdate/'2014-08'") ~> sparkRoutes ~> check {
        responseAs[String] must contain("The number of Posts in")
      }
    }

    "be able to retrieve the number of posts with a specific tag" in {
      Get("/count/tag/storm") ~> sparkRoutes ~> check {
        responseAs[String] must contain("The number of Posts tagged")
      }
    }

  }
}

trait Specs2RouteTest extends RouteTest with Specs2Interface
*/