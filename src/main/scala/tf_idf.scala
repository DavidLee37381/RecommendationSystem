import scala.collection.mutable.Map

object tf_idf {

  def idf_calc(query: List[String] , dataset :List[(String, String, String, String)]): Map[String, Double] =
  {
    val size = dataset.length
    var docCount: Map[String, Double] = Map.empty[String, Double].withDefaultValue(0.0)
    query.foreach(kword =>
      dataset.foreach( row =>
        if ( (row._1+ " " + row._2+ " " + row._3+ " " + row._4).contains(kword)) docCount(kword) = docCount(kword) + 1 ))

    docCount.foreach((s: (String, Double)) => docCount(s._1) = Math.log(size / s._2 ) )

    docCount
  }


  //TODO
  def tf_calc(): Unit ={
    //calculate the normalized frequency for each term of the query
    // norm frequencies = freq of the term in a row / lengh od the row
    //                    ^ this is from WordCount
  }

  //TODO
  def idf_tf_calc(): Unit ={
    //calls idf_calc
    //calls tf_calc
    //calculates the ranking
  }



}
