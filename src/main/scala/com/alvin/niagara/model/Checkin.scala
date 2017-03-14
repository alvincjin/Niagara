package com.alvin.niagara.model

import java.io.ByteArrayOutputStream
import java.util

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

import scala.collection.JavaConversions._
import scala.io.Source

/**
  * Created by alvinjin on 2017-03-14.
  */
case class Checkin(business_id: String, time: Seq[String], `type`: String)


object CheckinSerde {

  val avroSchema = Source.fromInputStream(getClass.getResourceAsStream("/schema/checkin.avsc")).mkString
  val schema = new Schema.Parser().parse(avroSchema)

  val reader = new GenericDatumReader[GenericRecord](schema)
  val writer = new GenericDatumWriter[GenericRecord](schema)

  def serialize(checkin: Checkin): Array[Byte] = {

    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get.binaryEncoder(out, null)

    val avroRecord = new GenericData.Record(schema)
    avroRecord.put("business_id", checkin.business_id)
    avroRecord.put("time", asJavaCollection(checkin.time))
    avroRecord.put("type", checkin.`type`)

    writer.write(avroRecord, encoder)
    encoder.flush
    out.close
    out.toByteArray

  }

  def deserialize(bytes: Array[Byte]): Checkin = {

    val decoder = DecoderFactory.get.binaryDecoder(bytes, null)
    val record = reader.read(null, decoder)

    val time = collectionAsScalaIterable(record.get("time")
      .asInstanceOf[util.Collection[AnyRef]])
      .map(_.toString).toList

    Checkin(
      record.get("business_id").toString,
      time,
      record.get("type").toString
    )
  }
}
