import scala.io.Source
import com.opencsv.CSVReader
import java.io.FileReader
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql._
import org.apache.spark.SparkContext, org.apache.spark.SparkConf

object CSVManager {



def importSP(path: String): DataFrame ={
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("CSVReader")
    .getOrCreate()

  val csvIm = spark.read.option("header", "true").csv(path)
  csvIm.select("title", "subtitle", "categories", "description")
}

  def importer(path: String):
  List[(String, String, String, String)] = {
    val csvfile = Source.fromFile(path) //import CSV file
    val lineList = csvfile.getLines().toList
    val wordList = lineList.map(x => x.split(",")).map(x => (x(2), x(3), x(5), x(7)))
    csvfile.close()
    wordList
  }

  // the new function to replace the function of importer, and to deal the bug of ”title1, title2“
  def importCsv(csvFile: String, columnIndices: List[Int]): List[Map[String, String]] = {
    val reader = new CSVReader(new FileReader(csvFile))
    val header = reader.readNext() // Assuming the first row contains column names

    try {
      var line: Array[String] = null
      val data = scala.collection.mutable.ListBuffer[Map[String, String]]()

      while ({ line = reader.readNext(); line != null }) {
        val rowData = columnIndices.map { columnIndex =>
          header(columnIndex) -> (if (columnIndex < line.length) line(columnIndex) else "")
        }.toMap
        data += rowData
      }

      data.toList
    } finally {
      reader.close()
    }
  }

  /**
   *
   * @param String
   * @return
   */
  def similarity(s: String): String ={
    var spl = s.split(" ")
    //spl.foreach(println)
    spl.foreach(w1  => if (w1.length > 3) {
      var p1 = spl.indexOf(w1)
      //  println(w1)
      spl.foreach(w2 =>
      if ((spl.indexOf(w1) != spl.indexOf(w2)) && (w2.length > 3) ){
        if (compare(w1,w2)) {
          var p2 = spl.indexOf(w2)
          if(w1.length > w2.length)
            spl(p1) = w2
          else
            spl(p2) = w1
        }
      })
      spl(p1) = toRoot(w1)
    })

    spl.mkString(" ")
  }

  def toRoot(w:String) : String ={
  var t = w
    if(t == "mice")
      t = "mouse"
    if (t == "teeth")
      t = "tooth"
    if (t == "children")
      t = "child"
    if (t == "women")
      t = "woman"
    if (t == "men")
      t = "man"
    if (t == "geese")
      t = "goose"
    if (t == "feet")
      t = "foot"
    if (t == "people")
      t = "person"
    if (t == "oxen")
      t = "ox"


    val exceptions = List("species", "series", "this", "mess", "s")

    if (t.endsWith("iness"))
      t = t.slice(0, w.length - 5).concat("y")
    if (t.endsWith("ness"))
      t = t.slice(0, w.length - 4)
    if (!exceptions.contains(t)) {
      if (t.endsWith("ies"))
        t = w.slice(0, w.length - 3).concat("y")
      if (t.endsWith("ves"))
        t = t.slice(0, w.length - 3).concat("f")
      if (t.endsWith("es"))
        t = t.slice(0, w.length - 2)
      if (t.endsWith("s"))
        t = t.slice(0, w.length - 1)
    }
    if (t.endsWith("ing"))
      t = t.slice(0, w.length - 3)
    if (t.endsWith("ed"))
      t = t.slice(0, w.length - 2)
    if (t.endsWith("er"))
      t = t.slice(0, w.length - 2)
    if (t.endsWith("est") && t!="best" && t!= "forest")
      t = t.slice(0, w.length - 3)

    t
  }

  def compare (w1:String, w2:String) : Boolean ={
    var min_s = math.min(w1.length, w2.length)
    var b = true
    for(i <- 0 until min_s) {
    //  println(w1(i) + " " +  w2(i))
      b = b && (w1(i) == w2(i)) }
    b
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
