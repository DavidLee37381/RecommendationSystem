object Main {

  def main(args: Array[String]): Unit = {

    println("Hello World!")

    val name = "Alice"
    println(s"My name is $name")

    val squared = square(2)
    println(s"2 squared is: $squared")

  }

  def square(x: Int): Int = {
    x * x
  }

}
