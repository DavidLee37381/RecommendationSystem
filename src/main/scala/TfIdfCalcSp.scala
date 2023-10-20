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
  def tfCalcSP(query: List[String], row: String, spark: SparkSession): RDD[(String, Double)] = {
    val docSize: Double = row.split(" ").length.toDouble

    val wCounter = WordUtilSp.wordCountSP(row, query).collectAsMap()
    val tf = mutable.Map.empty[String, Double].withDefaultValue(0.0)

    query.foreach { q =>
      val wordFreq = wCounter.getOrElse(q, 0).toDouble
      val normFreq = wordFreq / docSize
      tf(q) = normFreq
    }

    val tfRDD = spark.sparkContext.parallelize(tf.toSeq)
    tfRDD.collect().foreach {
      case (key, value) =>
        println(s" Key: $key, Value: $value")
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
    val ranks = Array.ofDim[Double](dataset.collect().length)
    val rdd = dataset.rdd
    var rowIndex = 0

    rdd.foreachPartition { partition =>
      // todo range: 0-4142211, partition values: [empty row]
      partition.foreach { row =>
        var tfIdf = 0.0
        query.map { q =>
          val text = row.mkString("")
          val idf = idf_val.filter(_._1 == q).map(_._2).take(1).headOption.getOrElse(0.0)
          val tf_v = tfCalcSP(query, text, spark)

          // we need to for row of document, we can caculate the term-frequncy of each word of query.
          // then we multiply the tf of the row for the idf

          tf_v.foreach { case (q, tf_v) =>
            tfIdf += idf * tf_v
          }
          ranks(rowIndex) = tfIdf
          if (tfIdf > 0) {
            println(f"Title: ${row(0)} \t Weights value: $tfIdf%.6f")
          }
          rowIndex += 1
        }
      }
    }

    val topN = 20
    val topNIndexes = ranks.zipWithIndex.sortBy(-_._1).take(topN).map(_._2)
    for (i <- 0 until topN) {
      println(s"${i + 1}. ${dataset.collect()(topNIndexes(i))(0)}")
    }
  }


}
