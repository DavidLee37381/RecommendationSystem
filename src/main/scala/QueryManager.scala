import scala.io.Source

object QueryManager {
  def getQuery(queryPath: String): List[String] = {
    val query = Source.fromFile(queryPath).getLines().toList.flatMap(l => l.split(","))
    query.map(w => CSVManager.toRoot(w))
  }
}
