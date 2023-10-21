import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

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
    /*
    idfRDD.collect().foreach {
      case (key, value) =>
        println(s"Key: $key, Value: $value")
    }
     */
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

    val wCounter = WordUtilSp.wordCountSP(row, query, spark).collectAsMap()
    val tf = mutable.Map.empty[String, Double].withDefaultValue(0.0)

    query.foreach { q =>
      val wordFreq = wCounter.getOrElse(q, 0).toDouble
      val normFreq = wordFreq / docSize
      tf(q) = normFreq
    }

    val tfRDD = spark.sparkContext.parallelize(tf.toSeq)
    /*
    tfRDD.collect().foreach {
      case (key, value) =>
        println(s" Key: $key, Value: $value")
    }
    */
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
    val idfMap = idfCalcSP(query, dataset, spark).collectAsMap()
    val size = 10 // for loop 10 rows data
    val ranks = new Array[Double](size)

    val limitedDataset = dataset.select("title", "subtitle", "categories", "description").limit(size)
    val tfValuesRdd = limitedDataset.rdd.collect()

    for (i <- 0 until size) {
      val row = tfValuesRdd(i)
      var tfIdf = 0.0
      val tfValues = tfCalcSP(query, row.mkString(""), spark).collectAsMap()
      tfValues.foreach { case (q, tf) =>
        tfIdf += idfMap.getOrElse(q, 0.0) * tf
      }
      ranks(i) = tfIdf
      if (tfIdf > 0) {
        val title = row.getAs[String]("title")
        println(f"Title: $title \t Weights value: $tfIdf%.6f")
      }
    }
    val topN = 10
    val topNIndexes = ranks.zipWithIndex.sortBy(-_._1).take(topN).map(_._2)
    for (i <- 0 until topN) {
      println(s"${i + 1}. ${dataset.collect()(topNIndexes(i))(0)}")
    }
  }

}
