import scala.io.Source

object CsvUtil {

  /**
   * funtion of readCsvFile
   * @param filePath
   * @return List
   */
  def readCsvFile(filePath: String): List[List[String]] = {
    val source = Source.fromFile(filePath)
    val lines = source.getLines().toList
    source.close()

    lines.map(_.split(",").toList)
  }

  def printCsvData(data: List[List[String]]): Unit = {
    data.foreach(row => println(row.mkString(", ")))
  }

  def toMapList(data: List[List[String]]): List[Map[String, String]] = {
    val header = data.head
    data.tail.map(row => header.zip(row).toMap)
  }

  def toListList(data: List[List[String]]): List[List[String]] = {
    data
  }
}