import scala.collection.mutable
import scala.collection.mutable.Map
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer

object TfIdfCalc {

  /**
   * inverse document frequency
   * @param query List
   * @param dataset
   * @return Map docCount
   */
  def idfCalc(query: List[String], dataset : List[mutable.Map[String, String]] /*List[(String, String, String, String)*/): mutable.Map[String, Double] =
  {
    val size = dataset.length
    var docCount: mutable.Map[String, Double] = mutable.Map.empty[String, Double].withDefaultValue(0.0)
    query.foreach(kword =>
      dataset.foreach( row =>
        if ( (row(constant.Columns(0)) + " " + row(constant.Columns(1))+ " "
            + row(constant.Columns(2))+ " " + row(constant.Columns(3))).contains(kword)) docCount(kword) = docCount(kword) + 1 ))

    docCount.foreach((s: (String, Double)) => docCount(s._1) = Math.log(size / s._2 ) )

    docCount
  }


  def idfCalcSP(query: List[String], dataset : List[mutable.Map[String, String]] /*List[(String, String, String, String)*/):
  RDD[(String, Double)] =
  {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("idfCalcSP")
      .getOrCreate()
    val size = dataset.length
    var docCount: mutable.Map[String, Double] = mutable.Map.empty[String, Double].withDefaultValue(0.0)
    query.foreach(kword =>
      dataset.foreach( row =>
        if ( (row(constant.Columns(0)) + " " + row(constant.Columns(1))+ " "
          + row(constant.Columns(2))+ " "
          + row(constant.Columns(3))).contains(kword)) docCount(kword) = docCount(kword) + 1 ))

    docCount.foreach((s: (String, Double)) => docCount(s._1) = Math.log(size / s._2 ) )

    spark.sparkContext.parallelize(docCount.toSeq)
  }


  /**
   * function of tf_calc: memorize a type of table in a file
   * term frequency
   * @param :
   * query: List[String], list of keyword
   * row : the document that is being analyzed
   * @return : mutable.Map[String, Double] -> Map(kWord -> tf_value, ...)
   */
  def tfCalc(query: List[String],row: String): mutable.Map[String, Double] ={
    val docSize: Double = row.split(" ").length.toDouble
    var wordFreq = 0.0
    var normFreq = 0.0
    val ris: mutable.Map[String, Double] = mutable.Map.empty[String, Double].withDefaultValue(0.0)


    val wCounter = WordUtil.wordCount(row, query).filter{case (k,v) => v!= 0}

      query.foreach { kWord =>
        if (wCounter != null && wCounter.size > 0)
          wordFreq = wCounter.getOrElse(kWord, 0).toDouble
        else
          wordFreq = 0.0
        normFreq = wordFreq / docSize
        ris += (kWord -> normFreq)
      }
    //calculate the normalized frequency for each term of the query
    // norm frequencies = freq of the term in a row / lengh od the row
    //                    ^ this is from WordCount
    ris
  }


def tfCalcSP(query: List[String],row: String): RDD[(String, Double)] ={
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("tfCalcSP")
    .getOrCreate()

  val docSize: Double = row.split(" ").length.toDouble
  var wordFreq = 0.0
  var normFreq = 0.0
  val ris: mutable.Map[String, Double] = mutable.Map.empty[String, Double].withDefaultValue(0.0)


  val wCounter = WordUtil.wordCountSP(row, query).filter{case (k,v) => v!= 0}

  query.foreach { kWord =>
      if (!wCounter.isEmpty())
        wordFreq = wCounter.filter(pair => pair._1==kWord).map(pair => pair._2).sum()
      else
        wordFreq = 0.0
      normFreq = wordFreq / docSize
      ris += (kWord -> normFreq)

    }
  //calculate the normalized frequency for each term of the query
  // norm frequencies = freq of the term in a row / lengh od the row
  //                    ^ this is from WordCount

  spark.sparkContext.parallelize(ris.toSeq)
}



  def idfTfCalc(query: List[String] , dataset :List[mutable.Map[String, String]]): Unit = {
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

    for( j <- 1 to 10)
    {
      max = 0
      for (i <- 1 until dataset.length-1) {
        if (ranks(max) < ranks(i) && !posList.contains(i))
          max = i
      }
      posList = posList.appended(max)

      println( j + ". " + dataset(max+1)(constant.Columns(0)))
    }


    //calls idf_calc
    //calls tf_calc
    //calculates the ranking
  }


/*  //TODO
  def idfTfCalc(query: List[String] , dataset :List[(String, String, String, String)]): Unit = {
    var idf_val = idfCalc(query, dataset)
    var ranks: Array[Double] = Array.empty
    var tf_v: mutable.Map[String, Double] = Map()

    for (i <- 1 until dataset.length) {
      var t = 0.0
      tf_v = tfCalc(query, dataset(i)._1 + dataset(i)._2 + dataset(i)._3 + dataset(i)._4)
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

    println( j + ". " + dataset(max+1)._1)
    }


    //calls idf_calc
    //calls tf_calc
    //calculates the ranking
  }

*/

}
