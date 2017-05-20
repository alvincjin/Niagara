package com.alvin.niagara.util

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord

/**
  * Created by alvinjin on 2017-05-05.
  */
case class MyRecord(f1:String) extends SpecificRecord {
  override def get(i: Int): AnyRef = ???

  override def put(i: Int, v: scala.Any): Unit = ???

  override def getSchema: Schema = ???
}
