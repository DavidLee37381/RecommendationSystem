import TfIdfCalc.{idfCalc, tfCalc}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object TfIdfCalcSp {

  /**
   * DataFrame == Dataset[Row]
   * .collect() gives us an Array of Rows instead (you can use it as a normal array)
   * R.toString, where R is a Row type gives us the String
   *
   * @param query
   * @param dataset
   * @return
   */
  def idfCalcSP(query: List[String], dataset: DataFrame, spark: SparkSession): RDD[(String, Double)] = {
    val size = dataset.count().toDouble
    val docCount = query.foldLeft(collection.mutable.Map.empty[String, Double].withDefaultValue(0.0)) { (acc, kword) =>
      val text = dataset.select("title", "subtitle", "categories", "description").collect()
      text.foreach { row =>
        val textString = row.mkString(" ")
        if (textString.contains(kword)) {
          acc(kword) += 1
        }
      }
      acc
    }
    val idfMap = docCount.transform((k, v) => Math.log(size / v))
    val idfRDD = spark.sparkContext.parallelize(idfMap.toSeq)
    idfRDD.collect().foreach {
      case (key, value) =>
        println(s"Key: $key, Value: $value")
    }
    idfRDD
  }

  /**
   * DataFrame == Dataset[Row]
   * .collect() gives us an Array of Rows instead (you can use it as a normal array)
   * R.toString, where R is a Row type gives us the String
   * Here I'm not sure which variable use to call the function
   *
   * @param query
   * @param row
   * @return
   */
  def tfCalcSP(query: List[String], dataset: DataFrame, spark: SparkSession): RDD[(String, Double)] = {
    val docSize = dataset.columns.length.toDouble

    val wordFreqMap = dataset.rdd.flatMap { row =>
      val text = row.mkString(" ")
      val words = text.split(" ").filter(query.contains)
      words.map(word => (word, 1))
    }.reduceByKey(_ + _).collectAsMap()

    val tfIdfList = query.map { kWord =>
      val wordFreq = wordFreqMap.getOrElse(kWord, 0)
      val normFreq = wordFreq / docSize
      (kWord, normFreq)
    }

    val tfRDD = spark.sparkContext.parallelize(tfIdfList)
    tfRDD.collect().foreach {
      case (key, value) =>
        println(s"Key: $key, Value: $value")
    }
    tfRDD
  }


  /**
   * DataFrame == Dataset[Row]
   * .collect() gives us an Array of Rows instead (you can use it as a normal array)
   * R.toString, where R is a Row type gives us the String
   *
   * @param query
   * @param dataset
   */
  def tfIdfCalcSP(query: List[String], dataset: DataFrame, spark: SparkSession): Unit = {
    val idf_val = idfCalcSP(query, dataset, spark).collect().toMap
    val tf_v = tfCalcSP(query, dataset, spark).collect().toMap
    val ranks = Array.ofDim[Double](dataset.columns.length)
    var rowIndex = 0
    val rdd = dataset.rdd
    rdd.foreachPartition { partition =>
      partition.foreach { row =>
        val tfIdf = query.map { q =>
          val idf = idf_val.filter(_._1 == q).map(_._2).take(1).headOption.getOrElse(0.0)
          val tf = tf_v.filter(_._1 == q).map(_._2).take(1).headOption.getOrElse(0.0)
          idf * tf
        }.sum
        ranks(rowIndex) = tfIdf
        if (tfIdf > 0) {
          println(f"Title: ${row(0)} \t Weights value: $tfIdf%.6f")
        }
        rowIndex += 1
      }
    }
    val topN = 20
    val topNIndexes = ranks.zipWithIndex.sortBy(-_._1).take(topN).map(_._2)
    for (i <- 0 until topN) {
      println(s"${i + 1}. ${dataset.columns(topNIndexes(i))}")
      // printf("%d. %s%n", j + 1, dataset(pos + 1)(constant.Columns(0)))
    }
  }


}
