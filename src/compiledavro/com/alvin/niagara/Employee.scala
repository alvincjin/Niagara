/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */
package com.alvin.niagara

import scala.annotation.switch

case class Employee(var firstName: String, var lastName: String, var age: Int, var phoneNumber: String) extends org.apache.avro.specific.SpecificRecordBase {
  def this() = this("", "", 0, "")
  def get(field$: Int): AnyRef = {
    (field$: @switch) match {
      case 0 => {
        firstName
      }.asInstanceOf[AnyRef]
      case 1 => {
        lastName
      }.asInstanceOf[AnyRef]
      case 2 => {
        age
      }.asInstanceOf[AnyRef]
      case 3 => {
        phoneNumber
      }.asInstanceOf[AnyRef]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
  }
  def put(field$: Int, value: Any): Unit = {
    (field$: @switch) match {
      case 0 => this.firstName = {
        value.toString
      }.asInstanceOf[String]
      case 1 => this.lastName = {
        value.toString
      }.asInstanceOf[String]
      case 2 => this.age = {
        value
      }.asInstanceOf[Int]
      case 3 => this.phoneNumber = {
        value.toString
      }.asInstanceOf[String]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
    ()
  }
  def getSchema: org.apache.avro.Schema = Employee.SCHEMA$
}

object Employee {
  val SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Employee\",\"namespace\":\"com.alvin.niagara\",\"fields\":[{\"name\":\"firstName\",\"type\":\"string\"},{\"name\":\"lastName\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"},{\"name\":\"phoneNumber\",\"type\":\"string\"}]}")
}