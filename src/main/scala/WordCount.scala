import scala.io.Source

object WordCount {

  /**
   * function of wordCount: count the appears times of each word.
   * input: part of csv file (s) and a list of keywords (k) ; output : map
   * this function wordCount is gived by Prof.
   *
   * @param s
   * @return
   */


  def wordCountScala(s: String, kList: List[String]): Map[String, Int] = {
    val exclude = Source.fromFile(constant.EXCLUDE_PATH).getLines().toList.flatMap(line => line.split(" "))
    var s1 = s.map(w => if (!(w.isLetter || w.isSpaceChar)) ' ' else w)
    s1 = s1.replace("  ", " ")
    s1 = s1.replace("  ", " ").toLowerCase()
    s1 = CSVManager.similarity(s1)
    var allWords = s1.split(" ")
    allWords = allWords.filterNot(element => exclude.contains(element))
    if(kList != null) allWords = allWords.filter(word => kList.contains(word))
    println("Frase da analizzare: " + s)
    //println("allWords da considerare: " + allWords)
    val emptyMap = Map[String, Int]() withDefaultValue 0
    val wc = allWords.foldLeft(emptyMap)((a, w) => a + (w -> (a(w) + 1)))

    wc
  }

}
