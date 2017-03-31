package com.alvin.niagara.dao

/**
  * Created by alvinjin on 2017-03-30.
  */

import com.alvin.niagara.config.DBManager

import org.joda.time.{DateTime, DateTimeZone}
import slick.driver.MySQLDriver.api._
import java.sql.Timestamp
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Random, Success}

object RestaurantDAO extends DBManager {

  case class Restaurant(
                         id: Option[Int] = None,
                         restaurantid: String,
                         restaurantname: String,
                         maxid: Long,
                         ingesttime: Timestamp
                       )

  //case class UserPayload(username: String, password: String, email: String)

  class Restaurants(tag: Tag) extends Table[Restaurant](tag, "restaurants") {

    def id = column[Int]("id", O.SqlType("SERIAL"), O.AutoInc, O.PrimaryKey)

    def restaurantid = column[String]("restaurantid", O.SqlType("VARCHAR(30)"))

    def restaurantname = column[String]("restaurantname", O.SqlType("VARCHAR(30)"))

    def maxid = column[Long]("maxid")

    def ingesttime = column[Timestamp]("createdAt", O.SqlType("timestamp not null default CURRENT_TIMESTAMP"))

    def * = (id.?, restaurantid, restaurantname, maxid, ingesttime) <>(Restaurant.tupled, Restaurant.unapply)

    def unique_user_idx = index("unique_user_id", restaurantid, unique = true)

  }

  lazy val restaurants = TableQuery[Restaurants]


  def createUsersTable = db.run(restaurants.schema.create)


  def insertRestaurant(restaurantid: String, restaurantname: String, maxid: Long, ingesttime: Timestamp): Future[Option[Restaurant]] = {


    val dt = new DateTime(DateTimeZone.forID("America/Toronto"))

    val restaurant = Restaurant(restaurantid = restaurantid, restaurantname = restaurantname,
      maxid = maxid, ingesttime = new Timestamp(dt.getMillis))

    val query = restaurants returning restaurants.map(_.id) into
      ((res, id) => restaurant.copy(id = Some(id)))

    val action = query += restaurant


    db.run(action.asTry).map { result =>
      result match {
        case Success(res) => Some(res)
        case Failure(e: Exception) => None
      }
    }
  }


  def queryUserById(handler: String): Future[Option[Long]] = {

    val action = restaurants.filter(_.restaurantid === handler).map(_.maxid).result.headOption

    /*db.run(action.asTry).map { result =>
      result match {
        case Success(res) => res
        case Failure(e: Exception) => None
      }
    }
    */

    db.run(action)


  }

}
