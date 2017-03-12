package com.alvin.niagara.model

import java.io.ByteArrayOutputStream
import java.util

import com.alvin.niagara.config.Config
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

import scala.collection.JavaConversions._
import scala.io.Source

/**
 * Created by jinc4 on 5/29/2016.
 *
 * Post case class object serializes/deserializes Object<->Avro
 */


case class PostTags(postid: Long, typeid: Int, tags: Seq[String], creationdate: Long)



object PostTags extends Config {

  val avroSchema = Source.fromInputStream(getClass.getResourceAsStream("/post.avsc")).mkString
  val schema = new Schema.Parser().parse(avroSchema)

  val reader = new GenericDatumReader[GenericRecord](PostTags.schema)
  val writer = new GenericDatumWriter[GenericRecord](PostTags.schema)

  /**
   * Serialize case class object to an Avro message
 *
   * @param post the given case class
   * @return An array byte to send
   */
  def serialize(post: PostTags): Array[Byte] = {

    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get.binaryEncoder(out, null)

    val avroRecord = new Record(PostTags.schema)
    avroRecord.put("postid", post.postid)
    avroRecord.put("typeid", post.typeid)
    avroRecord.put("tags", asJavaCollection(post.tags))
    avroRecord.put("creationdate", post.creationdate)

    writer.write(avroRecord, encoder)
    encoder.flush
    out.close
    out.toByteArray
  }

  /**
   * Deserialize an avro message to a case class object
 *
   * @param post the received byte array
   * @return  a case class object
   */
  def deserialize(post: Array[Byte]): PostTags = {

    val decoder = DecoderFactory.get.binaryDecoder(post, null)
    val record = reader.read(null, decoder)

    val tagList = collectionAsScalaIterable(record.get("tags")
      .asInstanceOf[util.Collection[AnyRef]])
      .map(_.toString).toList

    PostTags(record.get("postid").asInstanceOf[Long],
      record.get("typeid").asInstanceOf[Int],
      tagList,
      record.get("creationdate").asInstanceOf[Long]
    )
  }
}