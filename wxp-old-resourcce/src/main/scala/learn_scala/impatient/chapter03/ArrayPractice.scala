package learn_scala.impatient.chapter03

object ArrayPractice {

  def main(args: Array[String]): Unit = {

    import scala.collection.mutable.ArrayBuffer

    // 定义缓冲数组
    val b= ArrayBuffer[Int]()

    b+=0
    b+=(1,2,3)

    b ++= Array(4,5,6)
    println("foreach ex1 ")
    for(i <-0 until b.length) println(b(i))

    println("foreach ex2 ")
    for (elem <- b) println(elem)

    // for(...) yield
    val a=Array.range(1,5)
    val result = for (elem <- a if elem%2 == 0) yield 2 *elem
    println("foreach result ...")
    for (elem <- result) print(elem)








  }


}
