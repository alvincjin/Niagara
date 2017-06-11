/*
 * Copyright 2015 and onwards Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.alvin.niagara.sparkml

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import org.apache.spark.ml.feature.{CountVectorizer, IDF}
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class AssembleDocumentTermMatrix(private val spark: SparkSession) extends Serializable {

  import spark.implicits._


  /**
    * Broadcast stop words to each node, and create a NLP pipeline only each partition.
   */
  def contentsToTerms(docs: Dataset[(String, String)], stopWordsFile: String): Dataset[(String, Seq[String])] = {

    val stopWords = scala.io.Source.fromFile(stopWordsFile).getLines().toSet
    val bStopWords = spark.sparkContext.broadcast(stopWords)

    docs.mapPartitions { iter =>
      val pipeline = createNLPPipeline()
      iter.map { case (title, contents) => (title, plainTextToLemmas(contents, bStopWords.value, pipeline)) }
    }
  }

  /**
    * Create a StanfordCoreNLP pipeline object to lemmatize documents.
    */

  def createNLPPipeline(): StanfordCoreNLP = {
    val props = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma")
    new StanfordCoreNLP(props)
  }


  /**
    * Lemmatization is to combine different inflectional forms of a word into single term
    * @param text
    * @param stopWords
    * @param pipeline
    * @return
    */
  def plainTextToLemmas(text: String, stopWords: Set[String], pipeline: StanfordCoreNLP): Seq[String] = {

    val doc:Annotation = new Annotation(text)
    pipeline.annotate(doc)

    val sentences = doc.get(classOf[SentencesAnnotation])
    val lemmas = new ArrayBuffer[String]()

    for (sentence <- sentences.asScala;
         token <- sentence.get(classOf[TokensAnnotation]).asScala)
    {
      val lemma = token.get(classOf[LemmaAnnotation])
      if (lemma.length > 2 && !stopWords.contains(lemma) && isOnlyLetters(lemma)) {
        lemmas += lemma.toLowerCase
      }
    }
    lemmas
  }

  def isOnlyLetters(str: String): Boolean = {
    str.forall(c => Character.isLetter(c))
  }


  /**
   * Returns a document-term matrix where each element is the TF-IDF of the row's document and
   * the column's term.
   *
   * @param docTexts a DF with two columns: title and text
   */
  def documentTermMatrix(docTexts: Dataset[(String, String)], stopWordsFile: String, numTerms: Int)
    : (DataFrame, Array[String], Map[Long, String], Array[Double]) = {

    val terms = contentsToTerms(docTexts, stopWordsFile)

    val termsDF = terms.toDF("review_id", "terms")
    val filtered = termsDF.where(size($"terms") > 1)

    // Compute the term frequencies
    val countVectorizer = new CountVectorizer().
                          setInputCol("terms").
                          setOutputCol("termFreqs").
                          setVocabSize(numTerms)

    val vocabModel = countVectorizer.fit(filtered)

    //An array of terms with indices to trace the position
    val termIds = vocabModel.vocabulary

    //The value of each term is the number of times it appears in the review text
    val docTermFreqs = vocabModel.transform(filtered)
    docTermFreqs.cache()

    //add row id for each doc term frequencies
    val docIds = docTermFreqs.rdd.map(_.getString(0)).
                zipWithUniqueId().map(_.swap).
                collect().toMap

    val idf = new IDF().
      setInputCol("termFreqs").
      setOutputCol("tfidfVec")

    val idfModel = idf.fit(docTermFreqs)
    val docTermMatrix = idfModel.transform(docTermFreqs).select("review_id", "tfidfVec")

    (docTermMatrix, termIds, docIds, idfModel.idf.toArray)
  }

  /**
    * The top concepts are the concepts that explain the most variance in the dataset.
    * For each top concept, finds the terms that are most relevant to the concept.
    *
    * @param svd A singular value decomposition.
    * @param numConcepts The number of concepts to look at.
    * @param numTerms The number of terms to look at within each concept.
    * @param termIds The mapping of term IDs to terms.
    * @return A Seq of top concepts, in order, each with a Seq of top terms, in order.
    */
  def topTermsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
                            numTerms: Int, termIds: Array[String]): Seq[Seq[(String, Double)]] = {
    val v = svd.V
    val topTerms = new ArrayBuffer[Seq[(String, Double)]]()
    val arr = v.toArray
    for (i <- 0 until numConcepts) {
      val offs = i * v.numRows
      val termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex
      val sorted = termWeights.sortBy(-_._1)
      topTerms += sorted.take(numTerms).map {case (score, id) => (termIds(id), score) }
    }
    topTerms
  }

}
