import scala.collection.mutable.ListBuffer
object Main {

  def main(args: Array[String]): Unit = {

    // import dataset and print
    val wordExtracted = CSVManager.importCsv(constant.DATASET_CSV_PATH, List(2, 3, 5, 7) )
    println(wordExtracted.size)

    // wordCount
    wordExtracted.slice(0, 6).foreach(println)
    val queryPath = constant.QUERY_PATH
    val keywords = QueryManager.getQuery(queryPath).map(_.toLowerCase)

    // convert from a list of unmutable Maps to a ListBuffer of mutable maps
    val datalist = new ListBuffer[collection.mutable.Map[String, String]]()
    wordExtracted.foreach(map => datalist += map.to(collection.mutable.Map )) //let's fill the ListBuffer
    TfIdfCalc.idfTfCalc(keywords, datalist.toList) //.toList = converts from ListBuffer to List

  }
}
