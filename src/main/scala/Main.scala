import scala.io.Source

object Main {

  val exc: String = constant.EXCLUDE_PATH
  val path: String = constant.DATASET_CSV_PATH

  // wordcount in Scala
  def wordCountScala(s: String): Map[String, Int] = {

    val exclude = Source.fromFile(exc).getLines().toList.flatMap(line => line.split(" "))
    var s1 = s.map(w => if (!(w.isLetter || w.isSpaceChar)) ' ' else w)
    s1 = s1.replace("  ", " ")
    s1 = s1.replace("  ", " ").toLowerCase()
    s1 = CSVManager.similarity(s1)
    var allWords = s1.split(" ")
    allWords = allWords.filterNot(element => exclude.contains(element))
    println(s)
    val emptyMap = Map[String, Int]() withDefaultValue 0
    allWords.foldLeft(emptyMap)((a, w) => a + (w -> (a(w) + 1)))
  }



  def main(args: Array[String]): Unit = {

    val wordExtracted = CSVManager.importer(path)
    CSVManager.print(wordExtracted,1, 8)
    println(wordExtracted.size)
    wordExtracted.slice(0, 6).foreach(println)
    wordExtracted.slice(0, 6).foreach(n => println(wordCountScala(n._1 + " " + n._2 + " " + n._3 + " " + n._4)))

  }



}