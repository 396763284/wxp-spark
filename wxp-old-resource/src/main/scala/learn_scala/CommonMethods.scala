package learn_scala

object CommonMethods {

  def main(args: Array[String]): Unit = {
    //method_map
    method_flatten
  }

  def method_map(): Unit ={
    val array = List("a","b","c")
    array.map(_.toUpperCase).foreach(println)
    array.map(_.concat("dd")).foreach(println)
    val arr = Array.range(1,10)
    arr.map(math.pow(_,3)).foreach(println)
  }

  def method_flatten(): Unit ={
    val array = List(List(1,2),List(3,4)).flatten
    array.foreach(println)
  }

  def methfilter(): Unit ={
    var array= Array.range(1,10)
  }


}
