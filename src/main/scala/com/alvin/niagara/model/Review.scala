package com.alvin.niagara.model

import java.io.ByteArrayOutputStream

import com.sksamuel.avro4s.AvroSchema
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

import scala.io.Source

/**
  * Created by alvinjin on 2017-03-13.
  */
case class Review(business_id: String, cool: Long, date: String, funny: Long, review_id: String,
                  stars: Long, text: String, `type`: String, useful: Long, user_id: String)


object ReviewSerde {

  val schema: Schema = AvroSchema[Review]
  val reader = new GenericDatumReader[GenericRecord](schema)
  val writer = new GenericDatumWriter[GenericRecord](schema)

  def serialize(review: Review): Array[Byte] = {

    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get.binaryEncoder(out, null)

    val avroRecord = new GenericData.Record(schema)
    avroRecord.put("business_id", review.business_id)
    avroRecord.put("cool", review.cool)
    avroRecord.put("date", review.date)
    avroRecord.put("funny", review.funny)
    avroRecord.put("review_id", review.review_id)
    avroRecord.put("stars", review.stars)
    avroRecord.put("text", review.text)
    avroRecord.put("type", review.`type`)
    avroRecord.put("useful", review.useful)
    avroRecord.put("user_id", review.user_id)

    writer.write(avroRecord, encoder)
    encoder.flush
    out.close
    out.toByteArray

  }

  def deserialize(bytes: Array[Byte]): Review = {

    val decoder = DecoderFactory.get.binaryDecoder(bytes, null)
    val record = reader.read(null, decoder)

    Review(
      record.get("business_id").toString,
      record.get("cool").asInstanceOf[Long],
      record.get("date").toString,
      record.get("funny").asInstanceOf[Long],
      record.get("review_id").toString,
      record.get("stars").asInstanceOf[Long],
      record.get("text").toString,
      record.get("type").toString,
      record.get("useful").asInstanceOf[Long],
      record.get("user_id").toString
    )

  }

}
