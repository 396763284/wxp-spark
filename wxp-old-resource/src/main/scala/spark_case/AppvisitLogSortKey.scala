package spark_case

class AppvisitLogSortKey(val first:Long,val second:Long) extends Ordered[AppvisitLogSortKey] with Serializable{
  override def compare(that: AppvisitLogSortKey): Int = {
    if(this.first > that.first || (this.first == that.first && this.second > that.second))
    {
     return 1
    }else if(this.first < that.first || (this.first == that.first && this.second < that.second)) {
     return -1
    }else{
    return  0
    }
  }
}
