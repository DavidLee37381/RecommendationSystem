import TfIdfCalc.idfCalc
import org.scalatest.funsuite.AnyFunSuite

class FrequenceTest extends AnyFunSuite{
  test("TfIdfCalc.idfCalc"){
    val testResult = QueryManager.getQuery(constant.QUERY_PATH)
    println(testResult)
    val wordExtracted = CSVManager.importer(constant.DATASET_CSV_PATH)
    //print(wordExtracted)
    val idf = TfIdfCalc.idfCalc(testResult,wordExtracted)
    println(idf)
    println("IDF value: " + idf)
  }

  test("TfIdfCalc.tfCalc"){
    val qeuryResult = QueryManager.getQuery(constant.QUERY_PATH)
    println(qeuryResult)
    val wordExtracted = CSVManager.importer(constant.DATASET_CSV_PATH)

    val idf_val = idfCalc(qeuryResult, wordExtracted)
    var ranks: Array[Double] = Array.empty
    for (i <- 1 until wordExtracted.length) {
      var t = 0.0
      val tf_v = TfIdfCalc.tfCalc(qeuryResult, wordExtracted(i)._1 + wordExtracted(i)._2 + wordExtracted(i)._3 + wordExtracted(i)._4)
      qeuryResult.foreach(kword => t = t + (idf_val.getOrElse(kword, 0.0) * tf_v.getOrElse(kword, 0.0)))
      ranks = ranks :+ t
      if (t > 0) println(wordExtracted(i)._1 + " rank (t): " + t)
    }
  }


  test("TfIdfCalc.idfTfCalc"){
    val testResult = QueryManager.getQuery(constant.QUERY_PATH)
    println(testResult)
    val wordExtracted = CSVManager.importer(constant.DATASET_CSV_PATH)
    //print(wordExtracted)
    val idf = TfIdfCalc.idfTfCalc(testResult.map(_.toLowerCase),wordExtracted)
  }

}
