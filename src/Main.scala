import scala.io.Source
import org.apache.spark.SparkContext

object Main {

  val inputFile = "src/prova.txt"
  val exc = "src/ignore.txt"
  val path = "src/books.csv"

  // wordcount in Scala
  def wordCountScala(s: String) = {

    val exclude = Source.fromFile(exc).getLines().toList.flatMap(line => line.split(" "))
    var s1 = s.map(w => if (!(w.isLetter || w.isSpaceChar)) ' ' else w)
    s1 = s1.replace("  ", " ")
    s1 = s1.replace("  ", " ").toLowerCase()
    var allWords = s1.split(" ")
    allWords = allWords.filterNot(element => exclude.contains(element))
    println(s)
    val emptyMap = Map[String, Int]() withDefaultValue 0
    allWords.foldLeft(emptyMap)((a, w) => a + (w -> (a(w) + 1)))
  }

  def printCSV(list: List[(String, String, String, String)], i: Int): Unit = {
    if (i != 0) {
      def s = list.head
      println("Title: " + s._1 + "\nSubtitle: " + s._2 + "\nTag: " + s._3 + " \nDescription:" + s._4)
      printCSV(list.tail, i-1)
    }

  }

  def main(args: Array[String]): Unit = {

    val pcsv = Source.fromFile(path).getLines().toList
    val pcsvtxt = pcsv.map(x => x.split(",")).map(x => (x(2), x(3), x(5), x(7)))
    println(pcsvtxt.head)

    //pcsvtxt.foreach(s => println("Titolo: " + s._1 + "\nSottotitolo: " + s._2 + "\nTag: " + s._3 + " \nDescription:" + s._4))
    //printCSV(pcsvtxt.tail, 4)
    //println(wordCountScala(pcsvtxt(1)._1 + " " + pcsvtxt(1)._2 + " " + pcsvtxt(1)._3 + " " + pcsvtxt(1)._4))
    (1 to 4).foreach(n => println(wordCountScala(pcsvtxt(n)._1 + " " + pcsvtxt(n)._2 + " " + pcsvtxt(n)._3 + " " + pcsvtxt(n)._4))
      )


    println("Hello world!")
  }



}