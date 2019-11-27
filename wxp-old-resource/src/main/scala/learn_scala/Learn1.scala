package learn_scala

class Learn1 {

  //void函数
  def functionSecond():Unit={
    println("void function")
  }

  def getPi():Int={
    return 11;
  }
}
object First{
  //主函数
  def main(args: Array[String]): Unit = {
    //常量
    val b =3
    //变量
    var a:Int = 2
    println(a)
    if(a>1){
      println("yes")
    }else{
      println("no")
    }
    //函数调用
    println(functionFirst(1,3))
    //方法 或 闭包
    val multiplier= (i:Int)=> i*10
    println("闭包结果="+multiplier(1))
    //字符处理String
    //不可变string
    var greeting="Hello,World!"
    println(greeting)
    //可变string bulider
    val buf=new StringBuilder;
    buf+='a'
    buf++="bcdef"
    buf++="dd"
    println("buf="+buf.toString()+"--len="+greeting.length)
    println("连接"+greeting.concat("aaaa"))


    val map1=Map("scala"->1,"java"->2,"python"->3)
    val map2=Map(("scala",1),("java",2))
    println(map1+"--"+ map2)
    println( map1("scala"))
    //修改
    map1.getOrElse("c#",-1)


    //集合 seq ,set ,map
    val list1=List(1,2,3)
    val list2=0::list1
    println(list2)
    val list3=list1.::(0)
    var list5=list1.+:(0)





  }
  //带返回值函数
  def functionFirst(a:Int,b:Int):Int={
    var sum:Int=0
    sum=a+b
    return sum
  }




}