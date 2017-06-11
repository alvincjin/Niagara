package com.alvin.niagara.sparkml

import org.apache.spark.mllib.linalg.SingularValueDecomposition
import breeze.linalg.{DenseMatrix => BDenseMatrix, SparseVector => BSparseVector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrices, Matrix, SingularValueDecomposition, Vectors, Vector => MLLibVector}
/**
  * Created by alvinjin on 2017-06-10.
  */
class LSAQueryEngine(
                    val svd: SingularValueDecomposition[RowMatrix, Matrix],
                    val termIds: Array[String],
                    val docIds: Map[Long, String],
                    val termIdfs: Array[Double]
                    ) {
  //val VS: BDenseMatrix[Double] = multiplyByDiagonalMatrix(svd.V, svd.s)
  //val normalizedVS: BDenseMatrix[Double] = rowsNormalized(VS)

  val US: RowMatrix = multiplyByDiagonalRowMatrix(svd.U, svd.s)
  //val normalizedUS: RowMatrix = distributedRowsNormalized(US)

  val idTerms: Map[String, Int] = termIds.zipWithIndex.toMap
  //val idDocs: Map[String, Long] = docIds.map(_.swap)

  /**
    * Finds the product of a dense matrix and a diagonal matrix represented by a vector.
    * Breeze doesn't support efficient diagonal representations, so multiply manually.
    */
  def multiplyByDiagonalMatrix(mat: Matrix, diag: MLLibVector): BDenseMatrix[Double] = {
    val sArr = diag.toArray
    new BDenseMatrix[Double](mat.numRows, mat.numCols, mat.toArray)
      .mapPairs { case ((r, c), v) => v * sArr(c) }
  }


  /**
    * Finds the product of a distributed matrix and a diagonal matrix represented by a vector.
    */
  def multiplyByDiagonalRowMatrix(mat: RowMatrix, diag: MLLibVector): RowMatrix = {
    val sArr = diag.toArray
    new RowMatrix(mat.rows.map { vec =>
      val vecArr = vec.toArray
      val newArr = (0 until vec.size).toArray.map(i => vecArr(i) * sArr(i))
      Vectors.dense(newArr)
    })
  }

  /**
    * Returns a matrix where each row is divided by its length.
    */
  def rowsNormalized(mat: BDenseMatrix[Double]): BDenseMatrix[Double] = {
    val newMat = new BDenseMatrix[Double](mat.rows, mat.cols)
    for (r <- 0 until mat.rows) {
      val length = math.sqrt((0 until mat.cols).map(c => mat(r, c) * mat(r, c)).sum)
      (0 until mat.cols).foreach(c => newMat.update(r, c, mat(r, c) / length))
    }
    newMat
  }

  /**
    * Returns a distributed matrix where each row is divided by its length.
    */
  def distributedRowsNormalized(mat: RowMatrix): RowMatrix = {
    new RowMatrix(mat.rows.map { vec =>
      val array = vec.toArray
      val length = math.sqrt(array.map(x => x * x).sum)
      Vectors.dense(array.map(_ / length))
    })
  }


  /**
    * Finds docs relevant to a term query, represented as a vector with non-zero weights for the
    * terms in the query.
    */
  def topDocsForTermQuery(query: BSparseVector[Double]): Seq[(Double, Long)] = {
    val breezeV = new BDenseMatrix[Double](svd.V.numRows, svd.V.numCols, svd.V.toArray)
    val termRowArr = (breezeV.t * query).toArray

    val termRowVec = Matrices.dense(termRowArr.length, 1, termRowArr)

    // Compute scores against every doc
    val docScores = US.multiply(termRowVec)

    // Find the docs with the highest scores
    val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId
    allDocWeights.top(10)
  }

  /**
    * Builds a term query vector from a set of terms.
    */
  def termsToQueryVector(terms: Seq[String]): BSparseVector[Double] = {
    val indices = terms.map(idTerms(_)).toArray
    val values = indices.map(termIdfs(_))
    new BSparseVector[Double](indices, values, idTerms.size)
  }


  def printTopDocsForTermQuery(terms: Seq[String]): Unit = {
    val queryVec = termsToQueryVector(terms)
    val idWeights = topDocsForTermQuery(queryVec)
    println(idWeights.map { case (score, id) => (docIds(id), score) }.mkString(", "))
  }


}
