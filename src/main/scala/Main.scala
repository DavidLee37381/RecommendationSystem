
object Main {

  def main(args: Array[String]): Unit = {

    // import dataset and print
    val wordExtracted = CSVManager.importer(constant.DATASET_CSV_PATH)
    CSVManager.print(wordExtracted,1, 8)
    println(wordExtracted.size)

    // wordCount
    wordExtracted.slice(0, 6).foreach(println)
    val keywords = QueryManager.getQuery().map(_.toLowerCase)
    val idf = TfIdfCalc.idf_calc(keywords, wordExtracted)
    println("IDF value: " + idf)

/*
    val fPath = "dataset/counterFile.txt"
    var nDoc = 0

    // file counter cleaner
    WordUtil.cleanFile(fPath)

    wordExtracted.foreach{ n =>
      nDoc += 1
      val sRow = n._1 + " " + n._2 + " " + n._3 + " " + n._4
      WordUtil.printWordCounter(fPath,nDoc, WordUtil.wordCount(sRow, keywords))
      // ^rows from 0 to 6   ^current row                        ^ title     ^subtitle     ^ tag        ^description
      val tf = TfIdfCalc.tf_calc(keywords, sRow)
      println("tf " + tf)
    }*/

    TfIdfCalc.idf_tf_calc(keywords, wordExtracted)

  }
}
