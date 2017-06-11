package com.alvin.niagara.sparkml

import com.alvin.niagara.config.Config
import com.alvin.niagara.model.Review
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by alvinjin on 2017-06-06.
  */

object YelpRatingPredictor  extends App with Config {

  val spark = SparkSession
    .builder()
    .master(sparkMaster)
    .appName("YelpRecommanderApp")
    .getOrCreate()

  import spark.implicits._

  val assembleMatrix = new AssembleDocumentTermMatrix(spark)

  val reviewPath = yelpInputPath + "yelp_academic_dataset_review.json"

  val reviewDS: Dataset[Review] = spark.read.json(reviewPath).as[Review].
                                    filter(col("date") > "2016-07-01")

  val reviewTexts= reviewDS.select("review_id", "text").as[(String, String)]

  val numTerms = 100
  val (docTermMatrix, termIds, docIds, termIdfs) = assembleMatrix.documentTermMatrix(reviewTexts, "data/yelp/stopwords.txt", 200)

  docTermMatrix.cache()

  //Convert to rdd to utilize SVD to handle enormous matrix
  val vecRdd = docTermMatrix.select("tfidfVec").rdd.map { row =>
    Vectors.fromML(row.getAs[MLVector]("tfidfVec"))
  }

  vecRdd.cache()

  val mat = new RowMatrix(vecRdd)
  val k = 20 //this value is much smaller than numTerms
  val svd = mat.computeSVD(50, computeU=true)

  val topConceptTerms = assembleMatrix.topTermsInTopConcepts(svd, 10, 10, termIds)

  topConceptTerms.map(terms => println("Concept terms: "+terms.mkString(", ")))

  val queryEngine = new LSAQueryEngine(svd, termIds, docIds, termIdfs)
  queryEngine.printTopDocsForTermQuery(Seq("awesome", "burger"))


}