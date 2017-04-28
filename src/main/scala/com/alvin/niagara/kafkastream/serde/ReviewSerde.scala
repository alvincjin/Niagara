package com.alvin.niagara.kafkastream.serde

import java.io.ByteArrayOutputStream
import java.util

import com.alvin.niagara.model.{Business, Review}
import com.sksamuel.avro4s.AvroSchema
import org.apache.avro.Schema
import org.apache.avro.generic._
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.io.Source

/**
  * Created by alvinjin on 2017-04-01.
  */
class ReviewSerde extends Serde[Review]{

  val schema: Schema = AvroSchema[Review]
  val reader = new GenericDatumReader[GenericRecord](schema)
  val writer = new GenericDatumWriter[GenericRecord](schema)

  override def serializer(): Serializer[Review] = new Serializer[Review] {

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

    override def serialize(topic: String, data: Review): Array[Byte] = {
      val out = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get.binaryEncoder(out, null)

      val avroRecord = new GenericData.Record(schema)
      avroRecord.put("business_id", data.business_id)
      avroRecord.put("cool", data.cool)
      avroRecord.put("date", data.date)
      avroRecord.put("funny", data.funny)
      avroRecord.put("review_id", data.review_id)
      avroRecord.put("stars", data.stars)
      avroRecord.put("text", data.text)
      avroRecord.put("type", data.`type`)
      avroRecord.put("useful", data.useful)
      avroRecord.put("user_id", data.user_id)

      writer.write(avroRecord, encoder)
      encoder.flush
      out.close
      out.toByteArray
    }

    override def close(): Unit = {}
  }

  override def deserializer(): Deserializer[Review] = new Deserializer[Review] {

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

    override def close(): Unit = {}

    override def deserialize(topic: String, data: Array[Byte]): Review = {
      val decoder = DecoderFactory.get.binaryDecoder(data, null)
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

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}
}
