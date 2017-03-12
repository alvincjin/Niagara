package com.alvin.niagara.util

import java.text.SimpleDateFormat

import com.alvin.niagara.model.Post

import scala.xml.XML

/**
  * Created by alvin.jin on 3/2/2017.
  */
object XmlParser {

  /**
   * Parse a single line in the xml file
 *
   * @param line  the given line in xml
   * @return a option of Post
   */
  def parseXml(line: String): Option[Post] = {

    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")

    try {

      val xml = XML.loadString(line)
      val postId = (xml \ "@Id").text.toLong
      val postTypeId = (xml \ "@PostTypeId").text.toInt
      val title = (xml \ "@Title").text
      val creationDate = (xml \ "@CreationDate").text

      val creationDatetime = sdf.parse(creationDate).getTime

      Some(Post(postId, postTypeId, title, creationDatetime))

    } catch {
      case ex: Exception =>
        println(s"failed to parse XML in row: $line")
        None
    }
  }



}
