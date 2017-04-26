package com.alvin.niagara.kafkastream.serde

import java.io.ByteArrayOutputStream
import java.util

import com.alvin.niagara.model.User
import org.apache.avro.Schema
import org.apache.avro.generic._
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.collection.JavaConversions._
import scala.io.Source

/**
  * Created by alvinjin on 2017-04-05.
  */
class UserSerde extends Serde[User]{

  val avroSchema = Source.fromInputStream(getClass.getResourceAsStream("/schema/user.avsc")).mkString
  val schema = new Schema.Parser().parse(avroSchema)

  val reader = new GenericDatumReader[GenericRecord](schema)
  val writer = new GenericDatumWriter[GenericRecord](schema)

  override def serializer(): Serializer[User] = new Serializer[User] {

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

    override def serialize(topic: String, data: User): Array[Byte] = {
      val out = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get.binaryEncoder(out, null)

      val avroRecord = new GenericData.Record(schema)
      avroRecord.put("average_stars", data.average_stars)
      avroRecord.put("compliment_cool", data.compliment_cool)
      avroRecord.put("compliment_cute", data.compliment_cute)
      avroRecord.put("compliment_funny", data.compliment_funny)
      avroRecord.put("compliment_hot", data.compliment_hot)
      avroRecord.put("compliment_list", data.compliment_list)
      avroRecord.put("compliment_more", data.compliment_more)
      avroRecord.put("compliment_note", data.compliment_note)
      avroRecord.put("compliment_photos", data.compliment_photos)
      avroRecord.put("compliment_plain", data.compliment_plain)
      avroRecord.put("compliment_profile", data.compliment_profile)
      avroRecord.put("compliment_writer", data.compliment_writer)
      avroRecord.put("cool", data.cool)
      avroRecord.put("elite", asJavaCollection(data.elite))
      avroRecord.put("fans", data.fans)
      avroRecord.put("friends", asJavaCollection(data.friends))
      avroRecord.put("funny", data.funny)
      avroRecord.put("name", data.name)
      avroRecord.put("review_count", data.review_count)
      avroRecord.put("type", data.`type`)
      avroRecord.put("useful", data.useful)
      avroRecord.put("user_id", data.user_id)
      avroRecord.put("yelping_since", data.yelping_since)

      writer.write(avroRecord, encoder)
      encoder.flush
      out.close
      out.toByteArray
    }

    override def close(): Unit = {}
  }

  override def deserializer(): Deserializer[User] = new Deserializer[User] {

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

    override def close(): Unit = {}

    override def deserialize(topic: String, data: Array[Byte]): User = {

      val decoder = DecoderFactory.get.binaryDecoder(data, null)
      val record = reader.read(null, decoder)

      val elite = collectionAsScalaIterable(record.get("elite")
        .asInstanceOf[util.Collection[AnyRef]])
        .map(_.toString).toList


      val friends = collectionAsScalaIterable(record.get("friends")
        .asInstanceOf[util.Collection[AnyRef]])
        .map(_.toString).toList

      User( record.get("average_stars").asInstanceOf[Double],
        record.get("compliment_cool").asInstanceOf[Long],
        record.get("compliment_cute").asInstanceOf[Long],
        record.get("compliment_funny").asInstanceOf[Long],
        record.get("compliment_hot").asInstanceOf[Long],
        record.get("compliment_list").asInstanceOf[Long],
        record.get("compliment_more").asInstanceOf[Long],
        record.get("compliment_note").asInstanceOf[Long],
        record.get("compliment_photos").asInstanceOf[Long],
        record.get("compliment_plain").asInstanceOf[Long],
        record.get("compliment_profile").asInstanceOf[Long],
        record.get("compliment_writer").asInstanceOf[Long],
        record.get("cool").asInstanceOf[Long],
        elite,
        record.get("fans").asInstanceOf[Long],
        friends,
        record.get("funny").asInstanceOf[Long],
        record.get("name").toString,
        record.get("review_count").asInstanceOf[Long],
        record.get("type").toString,
        record.get("useful").asInstanceOf[Long],
        record.get("user_id").toString,
        record.get("yelping_since").toString
      )
    }
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}
}
