import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.io.Source


object WordUtilSp {

  /**
   * function of wordCount for Spark: count the appears times of each keyword.
   * input: part of csv file (s) and a list of keywords (k) ;
   * output : RDD (Term [String], Frequency[Int]);
   *
   * @param s
   * @return
   */
  def wordCountSP(s: String, kList: List[String], spark: SparkSession): RDD[(String, Int)] = {

    var source = Source.fromFile(constant.EXCLUDE_PATH)
    val exclude = source.getLines().toList.flatMap(line => line.split(" "))
    source.close()

    var s1 = s.map(w => if (!(w.isLetter || w.isSpaceChar)) ' ' else w)
    s1 = s1.replace("  ", " ")
    s1 = s1.replace("  ", " ").toLowerCase()
    s1 = CSVManager.similarity(s1)
    var allWords = s1.split(" ")
    allWords = allWords.filterNot(element => exclude.contains(element))
    if (kList != null) allWords = allWords.filter(word => kList.contains(word))
    if (allWords.isEmpty) return spark.sparkContext.emptyRDD
    val rdd0 = spark.sparkContext.parallelize(allWords)
    val rdd1 = rdd0.map(f => (f, 1))
    val rdd2 = rdd1.reduceByKey(_ + _)
    rdd2
  }
}
