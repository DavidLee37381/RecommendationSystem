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
    val idfMap = docCount.map(entry => {
      if (entry._2 > 0) (entry._1, Math.log(size/entry._2))
      else (entry._1, 0.0)
    })
    val idfRDD = spark.sparkContext.parallelize(idfMap.toSeq)
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
      var b = 0.0
      val a = wCounter.lookup(q)
      if (!(a.isEmpty)) b = a.head
      val normFreq = b / docSize
      tf(q) = normFreq
    }

    val tfRDD = spark.sparkContext.parallelize(tf.toSeq)
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
    val idfMap = idfCalcSP(query, dataset.limit(size), spark).collectAsMap()
    val ranks = new Array[Double](size)

    val limitedDataset = dataset.limit(size)
    val tfValuesRdd = limitedDataset.collect()

    for (i <- 0 until size) {
      val row = tfValuesRdd(i)
      var tfIdf = 0.0
      val tfValues = tfCalcSP(query, row.mkString(" ").replace("null", ""), spark).collectAsMap()
      query.foreach(q => {
        tfIdf += idfMap(q) * tfValues(q)
      } )
      ranks(i) = tfIdf
      if (tfIdf > 0) {
        val title = row.getAs[String]("title")
        println(f"Title: $title \t Weights value: $tfIdf%.6f")
      }
    }
    val topNIndexes = ranks.zipWithIndex.sortBy(-_._1).take(topN).map(_._2)
    val titles = limitedDataset.collect().map(row => row.getAs[String]("title"))
    for (i <- 0 until topN) {
      println(s"${i + 1}. ${titles(topNIndexes(i))}")
    }
  }

}
