package com.alvin.niagara.model

import java.io.ByteArrayOutputStream
import java.util

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

import scala.collection.JavaConversions._
import scala.io.Source

/**
  * Created by alvinjin on 2017-03-13.
  */
case class Business(address: String, attributes: Seq[String], business_id: String, categories: Seq[String],
                    city: String, hours: Seq[String], is_open: Long, latitude: Double, longitude: Double,
                    name: String, neighborhood: String, postal_code: String, review_count: Long, stars: Double,
                    state: String, `type`: String)


object BusinessSerde {

  val avroSchema = Source.fromInputStream(getClass.getResourceAsStream("/schema/business.avsc")).mkString
  val schema = new Schema.Parser().parse(avroSchema)

  val reader = new GenericDatumReader[GenericRecord](schema)
  val writer = new GenericDatumWriter[GenericRecord](schema)

  def serialize(buz: Business): Array[Byte] = {

    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get.binaryEncoder(out, null)

    val avroRecord = new GenericData.Record(schema)
    avroRecord.put("address", buz.address)
    avroRecord.put("attributes", asJavaCollection(buz.attributes))
    avroRecord.put("business_id", buz.business_id)
    avroRecord.put("categories", asJavaCollection(buz.attributes))
    avroRecord.put("city", buz.city)
    avroRecord.put("hours", asJavaCollection(buz.hours))
    avroRecord.put("is_open", buz.is_open)
    avroRecord.put("latitude", buz.latitude)
    avroRecord.put("longitude", buz.longitude)
    avroRecord.put("name", buz.name)
    avroRecord.put("neighborhood", buz.neighborhood)
    avroRecord.put("postal_code", buz.postal_code)
    avroRecord.put("review_count", buz.review_count)
    avroRecord.put("stars", buz.stars)
    avroRecord.put("state", buz.state)
    avroRecord.put("type", buz.`type`)



    writer.write(avroRecord, encoder)
    encoder.flush
    out.close
    out.toByteArray

  }

  def deserialize(bytes: Array[Byte]): Business = {

    val decoder = DecoderFactory.get.binaryDecoder(bytes, null)
    val record = reader.read(null, decoder)

    val attributes = collectionAsScalaIterable(record.get("attributes")
      .asInstanceOf[util.Collection[AnyRef]])
      .map(_.toString).toList

    val categories = collectionAsScalaIterable(record.get("categories")
      .asInstanceOf[util.Collection[AnyRef]])
      .map(_.toString).toList

    val hours = collectionAsScalaIterable(record.get("hours")
      .asInstanceOf[util.Collection[AnyRef]])
      .map(_.toString).toList

    Business(
      record.get("address").toString,
      attributes,
      record.get("business_id").toString,
      categories,
      record.get("city").toString,
      hours,
      record.get("is_open").asInstanceOf[Long],
      record.get("latitude").asInstanceOf[Double],
      record.get("longitude").asInstanceOf[Double],
      record.get("name").toString,
      record.get("neighborhood").toString,
      record.get("postal_code").toString,
      record.get("review_count").asInstanceOf[Long],
      record.get("stars").asInstanceOf[Double],
      record.get("state").toString,
      record.get("type").toString
    )

  }

}
