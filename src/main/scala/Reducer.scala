package scala

class Reducer {

  def reduce(key: String, values: Iterable[Int]): Int = {
    // Sum up all values for the key
    values.sum
  }

}

