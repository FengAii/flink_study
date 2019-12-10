package myFirstFrame

import scala.collection.mutable

object test_flatmap {

  def main(args: Array[String]): Unit = {

    var in = Seq("11,12","13,14")

    val stringses: Seq[Array[String]] = in.map(f => f.split(","))

    for (a <- stringses)
      println(a)

    println("====================")

    val strings: Seq[String] = in.flatMap(f => f.split(","))

    for (s <- strings)
      println(s)
  }

}
