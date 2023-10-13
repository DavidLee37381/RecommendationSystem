import scala.collection.mutable.ListBuffer

object Main {

  def main(args: Array[String]): Unit = {

    // import dataset and print
    val wordExtracted = CSVManager.importCsv(constant.DATASET_CSV_PATH, List(2, 3, 5, 7))
    println(wordExtracted.size)
    wordExtracted.slice(0, 6).foreach(println)

    // wordCount
    val queryPath = constant.QUERY_PATH
    val keywords = QueryManager.getQuery(queryPath).map(_.toLowerCase)

    // convert from a list Maps to a ListBuffer of mutable maps
    val datalist = new ListBuffer[collection.mutable.Map[String, String]]()
    wordExtracted.foreach(map => datalist += map.to(collection.mutable.Map))

    // document ranking using TF-IDF function.
    TfIdfCalc.idfTfCalc(keywords, datalist.toList)

  }
}
