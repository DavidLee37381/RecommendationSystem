package scala

object MapReduce {
  def main(args: Array[String]): Unit = {

    // Sample documents
    val documents = Seq("Hadoop MapReduce", "Scala functional programming")

    // Mapper instances
    val mappers = documents.map(doc => new Mapper().map(doc, doc))

    // Shuffle by grouping values per key
    val shuffled = mappers.flatMap(x => x).groupBy(_._1)

    // Reducer instances
    val reducers = shuffled.map { case (k, v) => new Reducer().reduce(k, Iterable(12)) }

    // Print output
    reducers.foreach(println)
  }
}
