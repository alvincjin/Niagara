package com.alvin.niagara.common

import java.io.ByteArrayOutputStream
import java.util
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericDatumReader, GenericData, GenericRecord, GenericDatumWriter}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}


import scala.collection.JavaConversions._
import scala.io.Source

/**
 * Created by jinc4 on 5/29/2016.
 */

case class Post(postId: Long, postTypeId: Int, tags: Seq[String], creationDate: Long) {

  lazy val toAvro = Post.toAvro(this)
}

object Post extends Settings {

  val avroSchema = Source.fromInputStream(getClass.getResourceAsStream("/post.avsc")).mkString

  val schema = new Schema.Parser().parse(avroSchema)

  val reader = new GenericDatumReader[GenericRecord](Post.schema)
  val writer = new GenericDatumWriter[GenericRecord](Post.schema)

  def toAvro(p: Post): Record = {
    val avroRecord = new Record(Post.schema)

    avroRecord.put("postId", p.postId)
    avroRecord.put("postTypeId", p.postTypeId)
    avroRecord.put("creationDate", p.creationDate)
    // convert to JavaCollection so Avro's GenericDatumWriter doesn't complain
    avroRecord.put("tags", asJavaCollection(p.tags))

    avroRecord
  }

  def toCaseClass(r: Record): Post = {

    val tagList = collectionAsScalaIterable(r.get("tags")
      .asInstanceOf[util.Collection[AnyRef]])
      .map(_.toString).toList

    Post(r.get("postId").asInstanceOf[Long],
      r.get("postTypeId").asInstanceOf[Int],
      tagList,
      r.get("creationDate").asInstanceOf[Long]
      // omg, seek help, find a scala/avro marshaling lib
    )
  }


  def serializeToAvro(post: Post): Array[Byte] = {

    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get.binaryEncoder(out, null)

    val avroRecord = new Record(Post.schema)
    avroRecord.put("postId", post.postId)
    avroRecord.put("postTypeId", post.postTypeId)
    avroRecord.put("tags", asJavaCollection(post.tags))
    avroRecord.put("creationDate", post.creationDate)
    // convert to JavaCollection so Avro's GenericDatumWriter doesn't complain

    writer.write(avroRecord, encoder)
    encoder.flush
    out.close
    out.toByteArray
  }


  def deserializeToClass(post: Array[Byte]): Post = {

    val decoder = DecoderFactory.get.binaryDecoder(post, null)
    val record = reader.read(null, decoder)

    val tagList = collectionAsScalaIterable(record.get("tags")
      .asInstanceOf[util.Collection[AnyRef]])
      .map(_.toString).toList

    Post(record.get("postId").asInstanceOf[Long],
      record.get("postTypeId").asInstanceOf[Int],
      tagList,
      record.get("creationDate").asInstanceOf[Long]
    )

  }
}