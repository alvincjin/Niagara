package com.alvin.niagara

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.{Matchers, FunSuite}

/**
 * Created by jinc4 on 6/6/2016.
 */

trait DatasetSuite extends FunSuite with Matchers {

  def equalDataFrames(expected: DataFrame, result: DataFrame) {

    assert(expected.schema.toString() === result.schema.toString)

    val expectedRDD = zipWithIndex(expected.rdd)
    val resultRDD = zipWithIndex(result.rdd)

    assert(expectedRDD.count() === resultRDD.count())

    val unequal = expectedRDD.cogroup(resultRDD)
      .filter{
        case (idx, (r1, r2)) =>
          !(r1.isEmpty || r2.isEmpty) && (!r1.head.equals(r2.head))
      }.collect()

    assert(unequal === List())
  }

  private def zipWithIndex[T](input: RDD[T]): RDD[(Int, T)] = {

    val counts = input
      .mapPartitions{itr => Iterator(itr.size)}
      .collect()

    val countSums = counts.scanLeft(0)(_ + _)
      .zipWithIndex.map{case (x, y) => (y, x)}
      .toMap

    input.mapPartitionsWithIndex{case (idx, itr) =>
      itr.zipWithIndex
        .map{
          case (y, i) => (i + countSums(idx), y)
        }
    }
  }



  def setNullableFields( df: DataFrame, nullable: Boolean) : DataFrame = {

    val schema = df.schema

    val newSchema = StructType(
      schema.map {
        case StructField( c, t, _, m) => StructField( c, t, nullable, m)
      })

    df.sqlContext.createDataFrame( df.rdd, newSchema )
  }




}
