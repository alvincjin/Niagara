package com.alvin.niagara.model

/**
  * Created by alvinjin on 2016-12-17.
  */

import slick.driver.MySQLDriver.api._

trait DBManager extends {

  def db = Database.forURL(
    url =
    driver =
  )

}
