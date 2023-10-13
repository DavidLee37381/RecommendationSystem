object MainSp {

  def main(args: Array[String]): Unit = {
    // import dataset and print
    val wordExtracted = CSVManagerSp.importSP(constant.DATASET_CSV_PATH)
    println(wordExtracted)

    // wordCount
    val queryPath = constant.QUERY_PATH
    val keywords = QueryManager.getQuery(queryPath).map(_.toLowerCase)
    wordExtracted.collect().foreach(
      row => WordUtilSp.wordCountSP(row.toString, keywords).
        collect().
        foreach(f =>
          println(f._1 + " | " + f._2))
    )

    //

  }
}
