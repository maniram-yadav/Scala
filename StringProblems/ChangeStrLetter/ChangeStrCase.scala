import scala.io.StdIn
import scala.Array._
import scala.collection.mutable.StringBuilder

object ChangeStrCase {
  
  def main(args:Array[String]){
     
    var input=scala.io.StdIn
    var str=input.readLine()
    var chstr:StringBuilder=new StringBuilder(str)
    var list=List[Char]()

    for(i <- 0 to str.length()-1)
    {
       if(str(i)>='A'&&str(i)<='Z')
          {
         
         chstr.update(i, str(i).toLower)
          }
       else
       {
           chstr.update(i, str(i).toUpper)
          
       } 
    }
    
    println(chstr)
    
  }
}
