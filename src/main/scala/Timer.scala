object Timer {

  sealed trait TimeUnit
  case object Milliseconds extends TimeUnit
  case object Microseconds extends TimeUnit
  case object Nanoseconds extends TimeUnit

  /**
   * Calculate the execution time of an operation
   * @param block The operation to measure time
   * @tparam R The return type of the operation
   * @return the result of the operation
   */
  def time[R](block: => R, logLevel: Int = 1, timeUnit: TimeUnit = Milliseconds): R = {
    val firstT = System.nanoTime()
    val res = block
    val secondT = System.nanoTime()
    val elapsed = (secondT - firstT)

    val timeInMilliseconds = timeUnit match {
      case Milliseconds => elapsed / 1000000.0
      case Microseconds => elapsed / 1000.0
      case Nanoseconds => elapsed.toDouble
    }

    if (logLevel > 0) {
      val unitName = timeUnit match {
        case Milliseconds => "ms"
        case Microseconds => "μs"
        case Nanoseconds => "ns"
      }

      println(s"\n ---------------------Time-------------------- \n Elapsed time: $timeInMilliseconds $unitName")
    }

    res
  }
}
