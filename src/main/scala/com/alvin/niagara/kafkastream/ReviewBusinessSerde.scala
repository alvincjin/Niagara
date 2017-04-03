package com.alvin.niagara.kafkastream


import java.io.ByteArrayOutputStream
import java.util

import com.alvin.niagara.model.Business
import org.apache.avro.Schema
import org.apache.avro.generic._
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.collection.JavaConversions._
import scala.io.Source

/**
  * Created by alvinjin on 2017-04-02.
  */

case class ReviewBusiness(business_id: String, date: String, review_id: String, stars: Long, text: String,
                          user_id: String, address: String, city: String, latitude: Double, longitude: Double,
                          name: String, postal_code: String, review_count: Long)

class ReviewBusinessSerde extends Serde[ReviewBusiness]{

  val avroSchema = Source.fromInputStream(getClass.getResourceAsStream("/schema/reviewbusiness.avsc")).mkString
  val schema = new Schema.Parser().parse(avroSchema)

  val reader = new GenericDatumReader[GenericRecord](schema)
  val writer = new GenericDatumWriter[GenericRecord](schema)

  override def serializer(): Serializer[ReviewBusiness] = new Serializer[ReviewBusiness] {

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

    override def serialize(topic: String, data: ReviewBusiness): Array[Byte] = {
      val out = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get.binaryEncoder(out, null)

      val avroRecord = new GenericData.Record(schema)
      avroRecord.put("business_id", data.business_id)
      avroRecord.put("date", data.date)
      avroRecord.put("review_id", data.review_id)
      avroRecord.put("stars", data.stars)
      avroRecord.put("text", data.text)
      avroRecord.put("user_id", data.user_id)
      avroRecord.put("address", data.address)
      avroRecord.put("city", data.city)
      avroRecord.put("latitude", data.latitude)
      avroRecord.put("longitude", data.longitude)
      avroRecord.put("name", data.name)
      avroRecord.put("postal_code", data.postal_code)
      avroRecord.put("review_count", data.review_count)

      writer.write(avroRecord, encoder)
      encoder.flush
      out.close
      out.toByteArray
    }

    override def close(): Unit = {}
  }

  override def deserializer(): Deserializer[ReviewBusiness] = new Deserializer[ReviewBusiness] {

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

    override def close(): Unit = {}

    override def deserialize(topic: String, data: Array[Byte]): ReviewBusiness = {
      val decoder = DecoderFactory.get.binaryDecoder(data, null)
      val record = reader.read(null, decoder)


      ReviewBusiness(
        record.get("business_id").toString,
        record.get("date").toString,
        record.get("review_id").toString,
        record.get("stars").asInstanceOf[Long],
        record.get("text").toString,
        record.get("user_id").toString,
        record.get("address").toString,
        record.get("city").toString,
        record.get("latitude").asInstanceOf[Double],
        record.get("longitude").asInstanceOf[Double],
        record.get("name").toString,
        record.get("postal_code").toString,
        record.get("review_count").asInstanceOf[Long]
      )
    }
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}
}
