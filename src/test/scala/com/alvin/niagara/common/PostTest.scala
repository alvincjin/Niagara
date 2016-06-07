package com.alvin.niagara.common

import com.alvin.niagara.SparkBase
import org.apache.avro.io.EncoderFactory
import org.scalatest._
import java.io.{ByteArrayOutputStream, File}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic._

/**
 * Created by jinc4 on 6/6/2016.
 */
class PostTest extends FunSuite with ShouldMatchers with SparkBase{

  val avroMessage: Array[Byte] = readAvroToByte("src/test/resources/post.avro",
                                                Post.schema)

  test("serializeToAvro should return a byte array")({

    val post = Post(11111L, 1, List("storm", "java"), 1407546091050L)
    val result: Array[Byte] = Post.serializeToAvro(post)

    assert(result === avroMessage)
  })

  test("deserializeToClass should return a case class object")({

    val expect = Post(11111L, 1, List("storm", "java"), 1407546091050L)
    val result: Post = Post.deserializeToClass(avroMessage)

    assert(expect === result)
  })


  def readAvroToByte(filename: String, schema: Schema):Array[Byte] = {

    val datumReader = new GenericDatumReader[GenericRecord](schema)
    val dataFileReader = new DataFileReader(new File(filename), datumReader)
    val record = dataFileReader.next().asInstanceOf[GenericData.Record]
    dataFileReader.close()

    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get.binaryEncoder(out, null)
    val writer = new GenericDatumWriter[GenericRecord](schema)
    writer.write(record, encoder)
    encoder.flush
    out.close
    out.toByteArray
  }

}
