import scala.io.Source

object CSVManager {

  def importer(path: String): List[(String, String, String, String)] =
  {
    val csvfile = Source.fromFile(path) //import CSV file
    val lineList = csvfile.getLines().toList
    val wordList = lineList.map(x => x.split(",")).map(x => (x(2), x(3), x(5), x(7)))
      return wordList
  }

  def printOld(wordList: List[(String, String, String, String)]): Unit ={
    wordList.foreach(s =>
      println("Title: " + s._1 + "\nSubtitle: " + s._2 + "\nTag: " + s._3 + " \nDescription:" + s._4 + "\n --------"))
  }

  def print(wordList: List[(String, String, String, String)], min :Int,  max : Int): Unit ={
    wordList.slice(min, max).foreach(s =>
      println("Title: " + s._1 + "\nSubtitle: " + s._2 + "\nTag: " + s._3 + " \nDescription:" + s._4 + "\n --------"))

  }


  def printAll(wl: List[(String, String, String, String)]): Unit ={
   print(wl, 0, wl.size)
  }

/*
  It prints the first n elements of the wordlist List
* */
  def printTill(wordList: List[(String, String, String, String)], n : Int): Unit ={
    wordList.slice(0, n).foreach(s =>
      println("Title: " + s._1 + "\nSubtitle: " + s._2 + "\nTag: " + s._3 + " \nDescription:" + s._4 + "\n --------"))

  }

}