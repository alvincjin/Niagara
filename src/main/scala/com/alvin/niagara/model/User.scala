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
case class User(average_stars: Double, compliment_cool: Long, compliment_cute: Long, compliment_funny: Long,
                compliment_hot: Long, compliment_list: Long, compliment_more: Long, compliment_note: Long,
                compliment_photos: Long, compliment_plain: Long, compliment_profile: Long, compliment_writer: Long,
                cool: Long, elite: Seq[String], fans: Long, friends: Seq[String], funny: Long, name: String,
                review_count: Long, `type`: String, useful: Long, user_id: String, yelping_since: String)


object UserSerde {

  val avroSchema = Source.fromInputStream(getClass.getResourceAsStream("/schema/user.avsc")).mkString
  val schema = new Schema.Parser().parse(avroSchema)

  val reader = new GenericDatumReader[GenericRecord](schema)
  val writer = new GenericDatumWriter[GenericRecord](schema)

  def serialize(user: User): Array[Byte] = {

    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get.binaryEncoder(out, null)

    val avroRecord = new GenericData.Record(schema)
    avroRecord.put("average_stars", user.average_stars)
    avroRecord.put("compliment_cool", user.compliment_cool)
    avroRecord.put("compliment_cute", user.compliment_cute)
    avroRecord.put("compliment_funny", user.compliment_funny)
    avroRecord.put("compliment_hot", user.compliment_hot)
    avroRecord.put("compliment_list", user.compliment_list)
    avroRecord.put("compliment_more", user.compliment_more)
    avroRecord.put("compliment_note", user.compliment_note)
    avroRecord.put("compliment_photos", user.compliment_photos)
    avroRecord.put("compliment_plain", user.compliment_plain)
    avroRecord.put("compliment_profile", user.compliment_profile)
    avroRecord.put("compliment_writer", user.compliment_writer)
    avroRecord.put("cool", user.cool)
    avroRecord.put("elite", asJavaCollection(user.elite))
    avroRecord.put("fans", user.fans)
    avroRecord.put("friends", asJavaCollection(user.friends))
    avroRecord.put("funny", user.funny)
    avroRecord.put("name", user.name)
    avroRecord.put("review_count", user.review_count)
    avroRecord.put("type", user.`type`)
    avroRecord.put("useful", user.useful)
    avroRecord.put("user_id", user.user_id)
    avroRecord.put("yelping_since", user.yelping_since)

    writer.write(avroRecord, encoder)
    encoder.flush
    out.close
    out.toByteArray

  }

  def deserialize(bytes: Array[Byte]): User = {

    val decoder = DecoderFactory.get.binaryDecoder(bytes, null)
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