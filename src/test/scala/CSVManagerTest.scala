import org.scalatest.funsuite.AnyFunSuite


class CSVManagerTest extends AnyFunSuite{
  /***
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

  test("CsvUtil.importCsv"){
    val columnsData = CsvUtil.importCsv(constant.DATASET_CSV_PATH, List(2, 3, 5, 7))
    println("Columns Data:")
    print(columnsData.foreach(column => println(column.mkString(", "))))
  }

  test("CSVManager.importCsv"){
    val columnIndices = List(2, 3, 5, 7) // Replace with the indices of the columns you want to extract (0-based)
    val data = CSVManager.importCsv(constant.DATASET_CSV_PATH, columnIndices)
    print (data.foreach(row => println(row)))
  }

}


