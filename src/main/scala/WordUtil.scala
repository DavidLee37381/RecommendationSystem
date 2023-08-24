import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.immutable.Map
import scala.io.Source

object WordUtil {

  /**
   * function of wordCount: count the appears times of each keyword.
   * input: part of csv file (s) and a list of keywords (k) ;
   * output : map (DocNumber[Int], Term [String],Frequency[Int]);
   * this function wordCount is given by Prof
   * @param s
   * @return
   */


  def wordCount(s: String, kList: List[String]): Map[String, Int] = {
    val exclude = Source.fromFile(constant.EXCLUDE_PATH).getLines().toList.flatMap(line => line.split(" "))
    var s1 = s.map(w => if (!(w.isLetter || w.isSpaceChar)) ' ' else w)
    s1 = s1.replace("  ", " ")
    s1 = s1.replace("  ", " ").toLowerCase()
    s1 = CSVManager.similarity(s1)
    var allWords = s1.split(" ")
    allWords = allWords.filterNot(element => exclude.contains(element))
    if (kList != null) allWords = allWords.filter(word => kList.contains(word))
    //println("Frase da analizzare: " + s)
    //println("allWords da considerare: " + allWords)
    val emptyMap = Map[String, Int]() withDefaultValue 0
    val wc = allWords.foldLeft(emptyMap)((a, w) => a + (w -> (a(w) + 1)))

    wc
  }

  /**
   * function of printWordCounter: memorize a type of table in a file
   * @param :
      * fPath: String, the path of the file where output is going to be saved
      * value: Map[String, Int] result from wordCounter obtained after analizing a document
   * @return : unit -> write the result in a file
   */
  def printWordCounter(fPath: String,docNumber : Int, value: Map[String, Int]) = {
    val f = new File(fPath)
    val bw = new BufferedWriter(new FileWriter(f,true))
    println("value: " + value)
    //write each row
    for((kWord, freq) <- value) {
      if(kWord != null ) {
        val newRow = s"$docNumber $kWord $freq\n"
        bw.append(newRow)
      }
    }
    bw.close()
  }

  /**
   * function of cleanFile: memorize a type of table in a file
   *
   * @param :     * fPath: String, the path of the file where output is going to be saved
   * @return : unit -> delete an existing file then create it new. A way to clean a file
   */
  def cleanFile (fPath: String) = {
    val file = new File(fPath)
    //pulizia manuale del file
    if (file.exists()) {
      file.delete()
      file.createNewFile()
    }
  }
}
