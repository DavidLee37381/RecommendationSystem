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
}
