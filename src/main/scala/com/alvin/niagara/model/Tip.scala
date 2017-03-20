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
case class Tip(business_id: String, date: String, likes: Long, text: String, `type`: String, user_id: String)


object TipSerde {

  val avroSchema = Source.fromInputStream(getClass.getResourceAsStream("/schema/tip.avsc")).mkString
  val schema = new Schema.Parser().parse(avroSchema)

  val reader = new GenericDatumReader[GenericRecord](schema)
  val writer = new GenericDatumWriter[GenericRecord](schema)

  def serialize(tip: Tip): Array[Byte] = {

    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get.binaryEncoder(out, null)

    val avroRecord = new GenericData.Record(schema)
    avroRecord.put("business_id", tip.business_id)
    avroRecord.put("date", tip.date)
    avroRecord.put("likes", tip.likes)
    avroRecord.put("text", tip.text)
    avroRecord.put("type", tip.`type`)
    avroRecord.put("user_id", tip.user_id)

    writer.write(avroRecord, encoder)
    encoder.flush
    out.close
    out.toByteArray

  }

  def deserialize(bytes: Array[Byte]): Tip = {

    val decoder = DecoderFactory.get.binaryDecoder(bytes, null)
    val record = reader.read(null, decoder)

    Tip(
      record.get("business_id").toString,
      record.get("date").toString,
      record.get("likes").asInstanceOf[Long],
      record.get("text").toString,
      record.get("type").toString,
      record.get("user_id").toString
    )
  }
}