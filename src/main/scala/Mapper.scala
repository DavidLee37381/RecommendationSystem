package scala

class Mapper {

  def map(key: String, value: String): Iterable[(String, Int)] = {
    // Split value into words
    val words = value.split(" ")

    // Emit each word with count of 1
    words.map(word => (word, 1))
  }

}