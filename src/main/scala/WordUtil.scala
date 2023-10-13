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
    val emptyMap = Map[String, Int]() withDefaultValue 0
    val wordnum = allWords.foldLeft(emptyMap)((a, w) => a + (w -> (a(w) + 1)))
    wordnum
  }

}
