package example
//import org.apache.log4j.{Level,Logger}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.rdd.RDD


/*

******************************************************************************************

  Convert the This scala file in jar package using sbt or any other tools and submit
  jar package using command
  
  spark-submit <jar package name>  <input file path> <output file path>  <string to be counteded in the input file (optionl )>
  
 
 Third argument to the jar package is optional. It will be used only when you want to search a particular string 

******************************************************************************************
*/

/**
  * Created by maniram on 24/1/18.
  */

object WordCount extends App {


//    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("WordCount").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val arglength = args.length
    if(arglength < 2)
         {throw new Exception("Invalid Argument submitted to the file")

}
        
   val rdd = sc.textFile(args(0).trim().toLowerCase())
    val words = rdd.flatMap(lines => lines.split("\\s+|,")).map(word => (word.toLowerCase(),1))
    var wordcount = words.reduceByKey(_+_)
    if(arglength>=3) {
      val countword = args(2).toLowerCase()
      wordcount = wordcount.filter { case (word, count) => word.equals(countword) }
    }

    wordcount.foreach(println)
    wordcount.saveAsTextFile(args(1).trim().toLowerCase())

     println("\n"*3+"--"*100+"\n")
     println("Information saved successfully at  path "+args(1).toLowerCase)
         println("\n"*3+"--"*100+"\n")

}
