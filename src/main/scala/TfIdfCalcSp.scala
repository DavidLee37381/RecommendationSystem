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

    val docCount: mutable.Map[String, Double] = mutable.Map.empty
    query.foreach( q => {
      println(q)
      docCount(q) = dataset.filter(r => {
        val a = (r.getString(0) + " " +
        r.getString(1) + " " +
        r.getString(2) + " " +
        r.getString(3)).toLowerCase
          .replace(",", "").replace("\"", "")
          .replace("-", " ").contains(q)
      if(a) println(r.getString(0))
      a
      }).count().toDouble
    }
    )
    /*dataset.filter(row =>
        row.getValuesMap(Seq("title", "subtitle", "categories", "description")).values.mkString(" ").toLowerCase.contains(q)).count().toDouble
    )*/

    // if (q == "minute") println(row)
    //row.mkString(" ").replace("null", "").toLowerCase.contains(q)
 /*   val docCount = query.foldLeft(collection.mutable.Map.empty[String, Double].withDefaultValue(0.0)) { (acc, kword) =>
    //  val text = dataset.select("title", "subtitle", "categories", "description").collect()

/*      dataset.filter(row => row.mkString(" ").toLowerCase.contains(kword)).foreach{ r =>
        acc(kword) += 1
      }*/
      acc(kword) = dataset.filter(row => row.mkString(" ").toLowerCase.contains(kword)).count().toDouble
/*      dataset.foreach{ row =>
        println(row)
        val textString = row.mkString(" ").toLowerCase.split(" ")
        if(textString.contains(kword)) acc(kword) += 1
      }*/
 /*     text.foreach { row =>
        val textString = row.mkString(" ")
        if (textString.contains(kword)) {
          acc(kword) += 1
        }
      }*/
      acc
    }*/


    val idfMap = docCount.map(entry => (entry._1, Math.log(size/entry._2)))
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

    val wCounter = WordUtilSp.wordCountSP(row, query, spark)//.collectAsMap()
    val tf = mutable.Map.empty[String, Double].withDefaultValue(0.0)

    query.foreach { q =>
      val a = wCounter.lookup(q).head
      //val wordFreq = wCounter.getOrElse(q, 0).toDouble
      val normFreq = a / docSize
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
  def tfIdfCalcSP(query: List[String], dataset: DataFrame, spark: SparkSession, size: Int, topN: Int): Unit = {
    val idfMap = idfCalcSP(query, dataset, spark).collectAsMap()
    //val size = size // for loop 10 rows data
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
    //val topN = topN
    val topNIndexes = ranks.zipWithIndex.sortBy(-_._1).take(topN).map(_._2)
    val titles = limitedDataset.collect().map(row => row.getAs[String]("title"))
    for (i <- 0 until topN) {
      println(s"${i + 1}. ${titles(topNIndexes(i))}")
    }
  }

}
