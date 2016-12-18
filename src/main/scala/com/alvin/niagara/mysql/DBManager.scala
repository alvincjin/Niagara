package com.alvin.niagara.mysql.model

/**
  * Created by alvinjin on 2016-12-17.
  */

import com.alvin.niagara.common.Setting
import com.alvin.niagara.mysql.MysqlConfig
import slick.driver.MySQLDriver.api._


trait DBManager {

  def db = Database.forURL(
    url = MysqlConfig.url,
    user = MysqlConfig.user,
    password = MysqlConfig.password,
    driver = MysqlConfig.dbDriver
  )

  implicit val session: Session = db.createSession()

}
