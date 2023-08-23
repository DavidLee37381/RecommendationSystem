
object Main {

  def main(args: Array[String]): Unit = {

    // import dataset and print
    val wordExtracted = CSVManager.importer(constant.DATASET_CSV_PATH)
    CSVManager.print(wordExtracted,1, 8)
    println(wordExtracted.size)

    // wordCount
    wordExtracted.slice(0, 6).foreach(println)
    //test1: filtering keywords
    //val keywords = List("Love", "Pain", "Happy").map(_.toLowerCase)
    val keywords = QueryManager.getQuery().map(_.toLowerCase)
    println(keywords)
    wordExtracted.foreach(n => println(WordCount.wordCountScala(n._1 + " " + n._2 + " " + n._3 + " " + n._4, keywords)))
    // ^rows from 0 to 6   ^current row                        ^ title     ^subtitle     ^ tag        ^description

  }
}
