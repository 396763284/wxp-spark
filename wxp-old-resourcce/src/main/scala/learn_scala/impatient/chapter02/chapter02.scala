package learn_scala.impatient.chapter02

object chapter02 {

  def main(args: Array[String]): Unit = {
    val x= 1

    val s= if (x>0) 1 else 0

    println("s="+s)
    // () 表示 Unit ,java 中 void
    val s2= if(x>1) 1 else ()
    println("s2="+s2)

    // 高级 for 循环
    println("for  example 1")
    for(i <- 1 to 3 ;j<- 1 to 3) print((10*i+j)+" ")

    // 任意定义
    println("for  example 2")
    for(i <- 1 to 3 ;j <- 1 to 3 if i==j) print((10*i+j)+" ")
    println("for  example 3")
    val s3= for(i <- 1 to 10 ) yield i%3
    println("s3="+s3)

    //函数
    // 方法对 对象进行操作， 函数不行


  }
}
