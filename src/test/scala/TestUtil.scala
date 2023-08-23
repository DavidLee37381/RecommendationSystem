import org.scalatest.funsuite.AnyFunSuite


class TestUtil extends AnyFunSuite{
  /**
   * this function has three questions:
   * 1. dont print the 0 row ( title subtitle description.....)
   * List((title,subtitle,categories,description), (Gilead, ,Fiction,"A NOVEL THAT READERS and critics have been eagerly anticipating for over a decade)
   * 2. when there are null, we need to define "null"
   * 3. the format of print is not good.
   * you can try run the test code and you will understand why
   */
  test("CSVManager.importer"){
    print(CSVManager.importer(constant.DATASET_CSV_PATH))
  }

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



}

