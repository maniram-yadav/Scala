import scala.io.StdIn

object ReverseString {
  
  def main(args:Array[String]){
    
    var in=scala.io.StdIn
    var str=in.readLine()
    var str2=str.reverse
    if(str.equals(str2))
      println("YES")
      else
        println("NO")
        
    
  }
  
}
