import scala.collection.mutable
import scala.collection.mutable.Map
object TfIdfCalc {

  def idf_calc(query: List[String] , dataset :List[(String, String, String, String)]): mutable.Map[String, Double] =
  {
    val size = dataset.length
    var docCount: mutable.Map[String, Double] = mutable.Map.empty[String, Double].withDefaultValue(0.0)
    query.foreach(kword =>
      dataset.foreach( row =>
        if ( (row._1+ " " + row._2+ " " + row._3+ " " + row._4).contains(kword)) docCount(kword) = docCount(kword) + 1 ))

    docCount.foreach((s: (String, Double)) => docCount(s._1) = Math.log(size / s._2 ) )

    docCount
  }


  /**
   * function of tf_calc: memorize a type of table in a file
   *
   * @param :
   * query: List[String], list of keyword
   * row : the document that is being analyzed
   * @return : mutable.Map[String, Double] -> Map(kWord -> tf_value, ...)
   */
  def tf_calc(query: List[String],row: String): mutable.Map[String, Double] ={
    val docSize: Double = row.size.toDouble
    var wordFreq: Double = 0.0
    var normFreq: Double = 0.0
    val ris: mutable.Map[String, Double] = mutable.Map.empty[String, Double].withDefaultValue(0.0)


    val wCounter = WordUtil.wordCount(row, query).filter{case (k,v) => v!= null}

      query.foreach { kWord =>
        if (wCounter != null && wCounter.size > 0) wordFreq = wCounter.getOrElse(kWord, 0).toDouble else wordFreq = 0.0
        normFreq = (wordFreq / docSize)
        ris += (kWord -> normFreq)
      }
    //calculate the normalized frequency for each term of the query
    // norm frequencies = freq of the term in a row / lengh od the row
    //                    ^ this is from WordCount
    ris
  }

  //TODO
  def idf_tf_calc(query: List[String] , dataset :List[(String, String, String, String)]): Unit = {
    var idf_val = idf_calc(query, dataset)
    var ranks: Array[Double] = Array.empty
    var tf_v: mutable.Map[String, Double] = Map()

    for (i <- 1 until dataset.length) {
      var t = 0.0
      tf_v = tf_calc(query, dataset(i)._1 + dataset(i)._2 + dataset(i)._3 + dataset(i)._4)
      query.foreach(kword => t = t + (idf_val.getOrElse(kword, 0.0) * tf_v.getOrElse(kword, 0.0)))
      ranks = ranks :+ t

      if (t > 0) println(dataset(i)._1 + " rank (t): " + t)

    }
    var posList: List[Int] = List.empty
    var max = 0

    for( j <- 1 to 10)
    {
      max = 0
    for (i <- 1 until dataset.length-1) {
      if (ranks(max) < ranks(i) && !posList.contains(i))
        max = i
    }
    posList = posList.appended(max)

    println( j + ". " + dataset(max)._1)
    }


    //calls idf_calc
    //calls tf_calc
    //calculates the ranking
  }



}
