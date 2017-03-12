package com.alvin.niagara.model

import java.io.ByteArrayOutputStream

import com.alvin.niagara.config.Config
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

import scala.io.Source
/**
  * Created by alvin.jin on 3/2/2017.
  */

case class NewPost(postid: Long, typeid: Int, title: String, creationdate: Long)

case class RichPost(postid: Long, posttype: String, title: String, creationdate: Long)

object PostSede extends Config {

  val avroSchema = Source.fromInputStream(getClass.getResourceAsStream("/newpost.avsc")).mkString
  val schema = new Schema.Parser().parse(avroSchema)

  val reader = new GenericDatumReader[GenericRecord](schema)
  val writer = new GenericDatumWriter[GenericRecord](schema)

  /**
   * Serialize case class object to an Avro message
 *
   * @param post the given case class
   * @return An array byte to send
   */
  def serialize(post: RichPost): Array[Byte] = {

    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get.binaryEncoder(out, null)

    val avroRecord = new Record(schema)
    avroRecord.put("postid", post.postid)
    avroRecord.put("posttype", post.posttype)
    avroRecord.put("title", post.title)
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

  def deserialize(post: Array[Byte]): RichPost = {

    val decoder = DecoderFactory.get.binaryDecoder(post, null)
    val record = reader.read(null, decoder)

    RichPost(record.get("postid").asInstanceOf[Long],
      record.get("posttype").toString,
      record.get("title").toString,//Avro deserializes string-based data to a class called Utf8
      record.get("creationdate").asInstanceOf[Long]
    )
  }
}