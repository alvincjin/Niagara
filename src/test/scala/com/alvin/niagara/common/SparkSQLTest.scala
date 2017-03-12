package com.alvin.niagara.common

import com.alvin.niagara.model.PostTags$
import com.alvin.niagara.spark.SparkSQL
import com.alvin.niagara.{DatasetSuite, SparkBase}
import org.scalatest._

/**
 * Created by jinc4 on 6/6/2016.
 */

class SparkSQLTest extends FunSuite with ShouldMatchers
  with DatasetSuite with SparkBase {


  import spark.implicits._
  val postDS = Seq(
    PostTags(11111L, 1, List("storm", "java"), 1407546091050L),
    PostTags(11112L, 1, List("storm", "java"), 1407513696417L),
    PostTags(11113L, 1, List("storm", "php","java"), 1407546091050L),
    PostTags(11114L, 2, List("storm"), 1407517189320L)
  ).toDS()

  test("CollectTagOverMonth should return a list of (month, count) for a specific tag")({

    val expect = Seq(("2014-08", 4)).toDF()
    val result = SparkSQL.countTagOverMonth(postDS, spark).toDF()

    equalDataFrames(setNullableFields(expect,true), setNullableFields(result,true))
  })


  test("CollectPostsByTag should return a dataset of post for a specific tag")({

    val expect = Seq(
      PostTags(11111L, 1, List("storm", "java"), 1407546091050L),
      PostTags(11112L, 1, List("storm", "java"), 1407513696417L),
      PostTags(11113L, 1, List("storm", "php","java"), 1407546091050L)
    ).toDF

    val result = SparkSQL.collectPostsByTag(postDS, "storm").toDF()

    equalDataFrames(setNullableFields(expect,true), setNullableFields(result,true))
  })


  test("findPopularMonth should return a tuple (month, count), which month with the most posts")({

    val expect = ("2014-08", 4)
    val result = SparkSQL.findPopularMonth(postDS, spark)
    assert(result === expect)
  })


}