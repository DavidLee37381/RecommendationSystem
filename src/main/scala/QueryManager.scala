import scala.io.Source

object QueryManager {
  def getQuery(queryPath: String): List[String] = {
    val query = Source.fromFile(queryPath).getLines().toList.flatMap(l => l.split(","))
    query.map(w => CSVManager.toRoot(w))
  }

  //computing the time cost of an operation
  def time[R](block: => R): R = {
    val firstT = System.nanoTime()
    val res = block
    val secondT = System.nanoTime()
    println("Elapsed time " + (secondT - firstT)/ 1000000 + "ms")
    res
  }
}
