import org.scalatest.funsuite.AnyFunSuite

class QueryManagerTest  extends AnyFunSuite{
  test(" QueryManager.getQuery"){
    val testResult = QueryManager.getQuery(constant.QUERY_PATH)
    print(testResult)
  }
}
