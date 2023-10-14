import org.apache.spark.sql.SparkSession

object MainSp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("Recommandation")
      .master("local[*]")
      .getOrCreate()

    try {
      // import dataset and print
      val wordExtracted = CSVManagerSp.importSP(constant.DATASET_CSV_PATH, spark)
      println(wordExtracted)

      val queryPath = constant.QUERY_PATH
      val keywords = QueryManager.getQuery(queryPath).map(_.toLowerCase)

      TfIdfCalcSp.tfIdfCalcSP(keywords, wordExtracted, spark)
    } finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }
}
