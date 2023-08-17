
object Main {

  val exc: String = constant.EXCLUDE_PATH
  val path: String = constant.DATASET_CSV_PATH

  def main(args: Array[String]): Unit = {

    // import dataset and print
    val wordExtracted = CSVManager.importer(path)
    CSVManager.print(wordExtracted,1, 8)
    println(wordExtracted.size)

    // wordCount
    wordExtracted.slice(0, 6).foreach(println)
    wordExtracted.slice(0, 6).foreach(n => println(WordCount.wordCountScala(n._1 + " " + n._2 + " " + n._3 + " " + n._4)))
    //            ^rows from 0 to 6   ^current row                          ^ title     ^subtitle     ^ tag        ^description
  }
}