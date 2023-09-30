import org.scalatest.FunSuite

class MathSuite extends FunSuite {

  test("add") {
    assert(1 + 1 === 2)
  }

  test("multiply") {
    assert(2 * 3 === 6)
  }
}
