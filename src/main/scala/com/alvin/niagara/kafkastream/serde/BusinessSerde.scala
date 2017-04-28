package com.alvin.niagara.kafkastream.serde

/**
  * Created by alvinjin on 2017-04-02.
  */


import java.io.ByteArrayOutputStream
import java.util

import com.alvin.niagara.model.Business
import com.sksamuel.avro4s.AvroSchema
import org.apache.avro.Schema
import org.apache.avro.generic._
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.collection.JavaConversions._
import scala.io.Source

/**
  * Created by alvinjin on 2017-04-02.
  */
class BusinessSerde extends Serde[Business]{

  val schema: Schema = AvroSchema[Business]

  val reader = new GenericDatumReader[GenericRecord](schema)
  val writer = new GenericDatumWriter[GenericRecord](schema)

  override def serializer(): Serializer[Business] = new Serializer[Business] {

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

    override def serialize(topic: String, data: Business): Array[Byte] = {
      val out = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get.binaryEncoder(out, null)

      val avroRecord = new GenericData.Record(schema)
      avroRecord.put("address", data.address)
      avroRecord.put("attributes", asJavaCollection(data.attributes.getOrElse(Seq.empty)))
      avroRecord.put("business_id", data.business_id)
      avroRecord.put("categories", asJavaCollection(data.categories.getOrElse(Seq.empty)))
      avroRecord.put("city", data.city)
      avroRecord.put("hours", asJavaCollection(data.hours.getOrElse(Seq.empty)))
      avroRecord.put("is_open", data.is_open)
      avroRecord.put("latitude", data.latitude)
      avroRecord.put("longitude", data.longitude)
      avroRecord.put("name", data.name)
      avroRecord.put("neighborhood", data.neighborhood)
      avroRecord.put("postal_code", data.postal_code)
      avroRecord.put("review_count", data.review_count)
      avroRecord.put("stars", data.stars)
      avroRecord.put("state", data.state)
      avroRecord.put("type", data.`type`)

      writer.write(avroRecord, encoder)
      encoder.flush
      out.close
      out.toByteArray
    }

    override def close(): Unit = {}
  }

  override def deserializer(): Deserializer[Business] = new Deserializer[Business] {

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

    override def close(): Unit = {}

    override def deserialize(topic: String, data: Array[Byte]): Business = {
      val decoder = DecoderFactory.get.binaryDecoder(data, null)
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
        if(attributes.isEmpty) None else Some(attributes),
        record.get("business_id").toString,
        if(categories.isEmpty) None else Some(categories),
        record.get("city").toString,
        if(hours.isEmpty) None else Some(hours),
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

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}
}
