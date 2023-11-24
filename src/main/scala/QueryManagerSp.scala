import org.apache.spark.sql.{DataFrame, SparkSession}

object QueryManagerSp {
  def getQuery(queryPath: String, spark: SparkSession): List[String] = {
    println(queryPath)
    val querySp = spark.read.textFile(queryPath).collect().mkString(",").split(",").toList
    querySp.map(w => CSVManager.toRoot(w))
  }

  //computing the time cost of an operation
  def time[R](block: => R): R = {
    val firstT = System.nanoTime()
    val res = block
    val secondT = System.nanoTime()
    println("Elapsed time " + (secondT - firstT) / 1000000 + "ms")
    res
  }
}
