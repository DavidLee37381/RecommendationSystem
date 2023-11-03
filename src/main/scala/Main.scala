object Main {

  def main(args: Array[String]): Unit = {
    Timer.time(mianf)
  }
  def mianf(): Unit = {
    // import dataset and print
    val wordExtracted = CSVManager.importCsv(constant.DATASET_CSV_PATH, List(2, 3, 5, 7))
    CSVManager.printWordExtracted(wordExtracted)
    // keywords from query
    val queryPath = constant.QUERY_PATH
    val keywords = QueryManager.getQuery(queryPath).map(_.toLowerCase)
    // Document ranking using TF-IDF function.
    val datalist = wordExtracted.map(_.to(collection.mutable.Map))
    TfIdfCalc.tfIdfCalc(keywords, datalist)
  }
}
