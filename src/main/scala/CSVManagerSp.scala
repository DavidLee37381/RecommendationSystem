import org.apache.spark.sql._

object CSVManagerSp {
  /**
   * CSVRead using scala
   * @param path the csvfile path
   * @param spark the spark session
   * @return dataframe csv
   */
  def importSP(path: String, spark: SparkSession): DataFrame ={
    val csvIm = spark.read.option("header", "true").csv(path)
    val selectedDF = csvIm.select("title", "subtitle", "categories", "description")
    selectedDF.show()
    selectedDF
  }
}

