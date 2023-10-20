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

    val wCounter = WordUtilSp.wordCountSP(row, query, spark).collectAsMap()
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


  /*
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
    */

  def tfIdfCalcSP(query: List[String], dataset: DataFrame, spark: SparkSession): Unit = {
    // 步骤 1：计算 IDF 值
    val idf_val = idfCalcSP(query, dataset, spark).collect().toMap

    // 步骤 2：初始化用于存储结果的列表
    var results = List[(String, Double)]()

    // 步骤 3：从 DataFrame 创建一个 RDD
    val rdd = dataset.rdd

    // 步骤 4：处理每个分区
    rdd.foreachPartition { partition =>
      partition.foreach { row =>
        val title = row.getString(2) // 假设标题在第一列
        val text = row.mkString("") // 将行转换为文本

        // 步骤 5：计算文档的 TF 值
        val tfValuesR = tfCalcSP(query, text, spark)
        val tfValues = tfValuesR.collectAsMap()

        // 步骤 6：计算每个查询词的 TF-IDF 值
        val tfIdfValues = query.map { q =>
          val idf = idf_val.getOrElse(q, 0.0)
          val tf = tfValues.getOrElse(q, 0.0)
          idf * tf
        }

        // 步骤 7：计算文档的总 TF-IDF 值
        val totalTfIdf = tfIdfValues.sum

        // 步骤 8：存储非零 TF-IDF 结果
        if (totalTfIdf > 0) {
          results = (title, totalTfIdf) :: results
        }
      }
    }

    // 步骤 9：打印前 N 个结果
    val topN = 20
    val topResults = results.sortBy(-_._2).take(topN)
    for ((title, tfIdf) <- topResults.zipWithIndex) {
      val rank = tfIdf + 1 // Add 1 to make the rank start from 1
      println(s"$rank. $title \t 权重值：$tfIdf%.6f")
    }
  }


}
