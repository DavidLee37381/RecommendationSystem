import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.typeOf
import scala.util.Random


class TestUtil extends AnyFunSuite{
  /**
   * this function has three questions:
   * 1. dont print the 0 row ( title subtitle description.....)
   * List((title,subtitle,categories,description), (Gilead, ,Fiction,"A NOVEL THAT READERS and critics have been eagerly anticipating for over a decade)
   * 2. when there are null, we need to define "null"
   * 3. the format of print is not good.
   * you can try run the test code and you will understand why
   */

  /**
   * test for CsvUtil
   */
  test("CsvUtil.readCsvFile"){
    val csvData = CsvUtil.readCsvFile(constant.DATASET_CSV_PATH)
    csvData.foreach(row => println(row.mkString(", ")))
  }

  test("CsvUtil.printCsvData"){
    val csvData = CsvUtil.readCsvFile(constant.DATASET_CSV_PATH)
    // Print CSV data
    println("CSV Data:")
    CsvUtil.printCsvData(csvData)
  }


  test("CsvUtil.toMapList"){
    val csvData = CsvUtil.readCsvFile(constant.DATASET_CSV_PATH)
    // Convert to Map list
    val mapList = CsvUtil.toMapList(csvData)
    println("\nCSV Data as Map List:")
    mapList.foreach(map => println(map))
  }

  test("CsvUtil.toListList"){
    val csvData = CsvUtil.readCsvFile(constant.DATASET_CSV_PATH)
    // Convert to List list
    val listList = CsvUtil.toListList(csvData)
    println("\nCSV Data as List List:")
    listList.foreach(list => println(list))

  }

  /**
   * tf test without spark tf = the count of special word in doc / the count of word in doc
   */
  test("TfIdfCalc.tfCalc") {
    val keywords = QueryManager.getQuery(constant.QUERY_PATH).map(_.toLowerCase)
    val csvData = CsvUtil.readCsvFile(constant.DATASET_CSV_PATH)
    val random = new Random()
    val selectedRows = random.shuffle(csvData).take(20) // This shuffles and selects a subset of rows
    val row = selectedRows.map(row => row.mkString("")).mkString("\n")
    println(row)
    val tf = TfIdfCalc.tfCalc(keywords, row)
    println(tf)
  }

  /**
   * idf test without spark
   * Inverse document frequency = log (total number of corpus documents / number of documents containing the word + 1)
   */
  test("TfIdfCalc.idfCalc") {
    val keywords = QueryManager.getQuery(constant.QUERY_PATH).map(_.toLowerCase)
    val wordExtracted = CSVManager.importCsv(constant.DATASET_CSV_PATH, List(2, 3, 5, 7))
    val idf = TfIdfCalc.idfCalc(keywords, wordExtracted.map(_.to(collection.mutable.Map)))
    println(idf)
  }

  /**
   * tf-idf test without spark tf * idf
   */
  test("TfIdfCalc.tfIdfCalc") {
    val keywords = QueryManager.getQuery(constant.QUERY_PATH).map(_.toLowerCase)
    val dataset = CSVManager.importCsv(constant.DATASET_CSV_PATH, List(2, 3, 5, 7))
    TfIdfCalc.tfIdfCalc(keywords, dataset.map(_.to(collection.mutable.Map)))
  }



  /**
   * CSVManagerSp.importSP
   */
  test("CSVManagerSp.importSP") {
    val spark: SparkSession = SparkSession.builder()
      .appName("importSP")
      .master("local[*]")
      .getOrCreate()
    val csvData = CSVManagerSp.importSP(constant.DATASET_CSV_PATH, spark)
    spark.close()
    println(csvData)
  }

  /**
   * TfIdfCalcSp.tfCalcSP
   */
  test("TfIdfCalcSp.tfCalcSP") {
    val spark: SparkSession = SparkSession.builder()
      .appName("tfCalcSP")
      .master("local[*]")
      .getOrCreate()

    val keywords = QueryManager.getQuery(constant.QUERY_PATH).map(_.toLowerCase)
    val csvData = CsvUtil.readCsvFile(constant.DATASET_CSV_PATH)
    val random = new Random()
    val selectedRows = random.shuffle(csvData).take(20) // This shuffles and selects a subset of rows
    val row = selectedRows.map(row => row.mkString("")).mkString("\n")
    println(row)
    val tfSP = TfIdfCalcSp.tfCalcSP(keywords, row, spark)
    println(tfSP)
    spark.close()
  }

  /**
   * TfIdfCalcSp.idfCalcSP
   */
  test("TfIdfCalcSp.idfCalcSP") {
    val spark: SparkSession = SparkSession.builder()
      .appName("idfCalcSP")
      .master("local[*]")
      .getOrCreate()

    val keywords = QueryManager.getQuery(constant.QUERY_PATH).map(_.toLowerCase)
    val dataFrame = CSVManagerSp.importSP(constant.DATASET_CSV_PATH, spark)
    val idfSP = TfIdfCalcSp.idfCalcSP(keywords, dataFrame, spark)
    println(idfSP)
    spark.close()
  }

  /**
   * TfIdfCalcSp.tfIdfCalcSP
   */
  test("TfIdfCalcSp.tfIdfCalcSP") {
    val spark: SparkSession = SparkSession.builder()
      .appName("tfIdfCalcSP")
      .master("local[*]")
      .getOrCreate()

    val keywords = QueryManager.getQuery(constant.QUERY_PATH).map(_.toLowerCase)
    val dataFrame = CSVManagerSp.importSP(constant.DATASET_CSV_PATH, spark)
    val tfIdf = TfIdfCalcSp.tfIdfCalcSP(keywords, dataFrame, spark)
    println(tfIdf)
    spark.close()
  }
}

