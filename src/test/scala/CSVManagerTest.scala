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

}

