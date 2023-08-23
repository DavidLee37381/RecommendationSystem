import scala.io.Source

object QueryManager {
  def getQuery(): List[String] = {
    var q = Source.fromFile(constant.QUERY_PATH).getLines().toList.flatMap(l => l.split(" "))
    q.map(w => CSVManager.toRoot(w))
  }
}
