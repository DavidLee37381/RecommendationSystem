import Main.mianf
import org.apache.spark.sql.SparkSession

object MainSp {
  def main(args: Array[String]): Unit = {
    Timer.time(mianf)
  }
  def mianf(): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("Recommandation")
      .master("local[*]")
      .getOrCreate()

      // import dataset and print
      val wordExtracted = CSVManagerSp.importSP(constant.DATASET_CSV_PATH, spark)
      println(wordExtracted)
      val queryPath = constant.QUERY_PATH
      val keywords = QueryManager.getQuery(queryPath).map(_.toLowerCase)

      TfIdfCalcSp.tfIdfCalcSP(keywords, wordExtracted, spark, 6810, 10)

      if (spark != null) {
        spark.stop()
      }

  }
}
