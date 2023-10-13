import org.apache.spark.sql._

object CSVManagerSp {
  def importSP(path: String): DataFrame ={
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("CSVReader")
      .getOrCreate()

    val csvIm = spark.read.option("header", "true").csv(path)
    csvIm.select("title", "subtitle", "categories", "description")
  }
}
