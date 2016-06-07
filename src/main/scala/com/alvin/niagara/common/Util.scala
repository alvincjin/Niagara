package com.alvin.niagara.common

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.{Dataset, SaveMode}
import scala.xml.XML

/**
 * Created by JINC4 on 5/27/2016.
 *
 * A utility object contains methods to parsexml, IO, formatting, etc
 */
object Util {

  /**
   * Parse a single line in the xml file
   * @param line  the given line in xml
   * @param sdf the simpleDataFormat object
   * @return a option of Post
   */
  def parseXml(line: String, sdf: SimpleDateFormat): Option[Post] = {
    try {

      val xml = XML.loadString(line)
      val postId = (xml \ "@Id").text.toLong
      val postTypeId = (xml \ "@PostTypeId").text.toInt
      val creationDate = (xml \ "@CreationDate").text
      val tags = (xml \ "@Tags").text


      val creationDatetime = sdf.parse(creationDate).getTime

      val tagList = if (tags.length == 0) List[String]()
      else tags.substring(1, tags.length - 1).split("><").toList

      Some(Post(postId, postTypeId, tagList, creationDatetime))

    } catch {
      case ex: Exception =>
        println(s"failed to parse XML in row: $line")
        None
    }
  }


  /**
   * Writes the given dataframe into parquet format
   * @param dataset a dataframe object
   * @param path  the output location
   */
  def writeParquet(dataset: Dataset[Post], path: String): Unit = {

    dataset.toDF().write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(path)
  }

  /**
   * Convert unix timestamp to a yyyy-MM format
   * @param ts the given long type timestamp
   * @return a literal timestamp in yyyy-MM
   */
  def getYearMonth(ts: Long): String = new SimpleDateFormat("yyyy-MM").format(new Date(ts))

}