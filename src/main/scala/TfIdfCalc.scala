import scala.collection.mutable

object TfIdfCalc {

  /**
   * inverse document frequency
   * 逆文档频率
   *
   * @param query List
   * @param dataset
   * @return Map docCount
   */
  def idfCalc(query: List[String], dataset: List[mutable.Map[String, String]]): mutable.Map[String, Double] = {
    val size = dataset.length
    var docCount: mutable.Map[String, Double] = mutable.Map.empty[String, Double].withDefaultValue(0.0)
    query.foreach(kword =>
      dataset.foreach(row =>
        if ((row("title") + " " + row("subtitle") + " "
          + row("categories") + " " + row("description")).contains(kword)) docCount(kword) = docCount(kword) + 1))
    docCount.foreach((s: (String, Double)) => docCount(s._1) = Math.log(size / s._2))
    docCount
  }


  /**
   * function of tf_calc: memorize a type of table in a file
   * term frequency 词频
   *
   * @param :
                                                                                                * query: List[String], list of keyword
   *        row : the document that is being analyzed
   * @return : mutable.Map[String, Double] -> Map(kWord -> tf_value, ...)
   */
  def tfCalc(query: List[String], row: String): mutable.Map[String, Double] = {
    val docSize: Double = row.split(" ").length.toDouble
    var wordFreq = 0.0
    var normFreq = 0.0
    val ris: mutable.Map[String, Double] = mutable.Map.empty[String, Double].withDefaultValue(0.0)
    val wCounter = WordUtil.wordCount(row, query).filter { case (k, v) => v != null }
    query.foreach { kWord =>
      if (wCounter != null && wCounter.nonEmpty)
        wordFreq = wCounter.getOrElse(kWord, 0).toDouble
      else
        wordFreq = 0.0
      normFreq = wordFreq / docSize
      ris += (kWord -> normFreq)
    }
    ris
  }

  /**
   * Ranking and the weights value of keywords
   * A high weight in tf–idf is reached by a high term frequency (in the given document) and a low document frequency
   * of the term in the whole collection of documents; the weights hence tend to filter out common terms.
   *
   * @param query
   * @param dataset
   */
  def idfTfCalc(query: List[String], dataset: List[mutable.Map[String, String]]): Unit = {
    var idf_val = idfCalc(query, dataset)
    var ranks: Array[Double] = Array.empty
    var tf_v: mutable.Map[String, Double] = mutable.Map()
    println(dataset.size)
    for (i <- 1 until dataset.length) {
      var t = 0.0
      tf_v = tfCalc(query, dataset(i)(constant.Columns(0)) + dataset(i)(constant.Columns(1))
        + dataset(i)(constant.Columns(2)) + dataset(i)(constant.Columns(3)))
      query.foreach(kword => t = t + (idf_val.getOrElse(kword, 0.0) * tf_v.getOrElse(kword, 0.0)))
      ranks = ranks :+ t
      if (t > 0) println(dataset(i)(constant.Columns(0)) + " rank (t): " + t)

    }
    var posList: List[Int] = List.empty
    var max = 0
    for (j <- 1 to 10) {
      max = 0
      for (i <- 1 until dataset.length - 1) {
        if (ranks(max) < ranks(i) && !posList.contains(i))
          max = i
      }
      posList = posList.appended(max)
      println(j + ". " + dataset(max + 1)(constant.Columns(0)))
    }
  }

}
