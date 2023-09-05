import com.opencsv.CSVReader

import java.io.FileReader
import scala.collection.mutable.ListBuffer
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

  def importCsv(csvFile: String, columnIndices: List[Int]): List[List[String]] = {
    val reader = new CSVReader(new FileReader(csvFile))
    val columnValues = ListBuffer[List[String]]()

    try {
      var line: Array[String] = null
      while ({ line = reader.readNext(); line != null }) {
        val selectedColumns = columnIndices.map { columnIndex =>
          if (columnIndex < line.length) {
            line(columnIndex)
          } else {
            "" // Add an empty string for missing values
          }
        }
        columnValues += selectedColumns
      }
    } finally {
      reader.close()
    }
    columnValues.toList
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