import com.opencsv.CSVReader
import java.io.{BufferedReader, FileReader}
import scala.collection.mutable.ListBuffer

object CSVManager {

  /**
   * importCsv use the function of scala
   * @param csvFile
   * @param columnIndices
   * @return
   */
  def importCsv(csvFile: String, columnIndices: List[Int]): List[Map[String, String]] = {
    var reader: CSVReader = null
    val data = ListBuffer[Map[String, String]]()

    try {
      reader = new CSVReader(new BufferedReader(new FileReader(csvFile)))
      val header = reader.readNext() // Assuming the first row contains column names
      var line: Array[String] = null

      while ( {
        line = reader.readNext(); line != null
      }) {
        val rowData = columnIndices.map { columnIndex =>
          header(columnIndex) -> (if (columnIndex < line.length) line(columnIndex) else "")
        }.toMap
        data += rowData
      }
    } finally {
      if (reader != null) {
        reader.close()
      }
    }

    data.toList
  }


  /**
   * compare the words are same or not
   * @param s word
   * @return
   */
  def similarity(s: String): String = {
    val words = s.split(" ")
    val result = words.map { word1 =>
      if (word1.length > 3) {
        val similarWords = words.filter(word2 =>
          word2.length > 3 && word1 != word2 && compare(word1, word2)
        )
        if (similarWords.nonEmpty) {
          val mostSimilarWord = similarWords.maxBy(_.length)
          toRoot(mostSimilarWord)
        } else {
          toRoot(word1)
        }
      } else {
        word1
      }
    }

    result.mkString(" ")
  }


  def toRoot(w: String): String = {
    val rootWords = Map(
      "mice" -> "mouse",
      "teeth" -> "tooth",
      "children" -> "child",
      "women" -> "woman",
      "men" -> "man",
      "geese" -> "goose",
      "feet" -> "foot",
      "people" -> "person",
      "oxen" -> "ox"
    )

    val exceptions = Set("species", "series", "this", "mess", "s")

    val transformedWord = w match {
      case s if rootWords.contains(s) => rootWords(s)
      case s if s.endsWith("iness") => s.dropRight(5) + "y"
      case s if s.endsWith("ness") => s.dropRight(4)
      case s if !exceptions.contains(s) =>
        s match {
          case str if str.endsWith("ies") => str.dropRight(3) + "y"
          case str if str.endsWith("ves") => str.dropRight(3) + "f"
          case str if str.endsWith("es") => str.dropRight(2)
          case str if str.endsWith("s") => str.dropRight(1)
          case _ => s
        }
      case s if s.endsWith("ing") => s.dropRight(3)
      case s if s.endsWith("ed") => s.dropRight(2)
      case s if s.endsWith("er") => s.dropRight(2)
      case s if s.endsWith("est") && s != "best" && s != "forest" => s.dropRight(3)
      case _ => w
    }

    transformedWord
  }

  def compare(w1: String, w2: String): Boolean = {
    w1.zip(w2).forall { case (c1, c2) => c1 == c2 }
  }

  /**
   *
   * @param wordExtracted
   */
  def printWordExtracted(wordExtracted: List[Map[String, String]]): Unit = {
    for ((entry, index) <- wordExtracted.zipWithIndex) {
      val title = entry.getOrElse("title", "")
      val subtitle = entry.getOrElse("subtitle", "")
      val categories = entry.getOrElse("categories", "")
      val description = entry.getOrElse("description", "")
      println(s"Title: $title\nsubtitle: $subtitle\ncategories: $categories\nDescription: $description\n------------------------")
    }
  }

}
