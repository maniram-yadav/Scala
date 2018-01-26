package mypc.spark.codes.sql

/**
  * Created by maniram on 19/1/18.
  */

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.log4j.{Logger,Level}
import org.apache.spark.sql.{Row,SQLContext,SparkSession}
import org.apache.spark.sql.types.{StringType,StructType,StructField,IntegerType}
import org.apache.hadoop.io.Text
//import org.apache.spark.sql.SQLContext.implicits._

object defineSchema {


  //def main(args :Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val session = SparkSession.builder().appName("DefSchema").master("local[1]").getOrCreate()
    val context = session.sparkContext

    //val conf = new SparkConf().setAppName("selfDefSchema").setMaster("local[1]")
    //val sc = new SparkContext(conf)

    val rdd = context.textFile("/home/maniram/data/emp")
    //rdd foreach println
   var schemaString = "id name sal role"
   var defSchema = new StructType(
     schemaString.split(" ").map(field => StructField(field,StringType,true))
   )
    //schemaString.split(" ").map(field => StructField(field,StringType,true))
//schemaString.split(" ").map(field => StructField(field,if(field=="id"||field=="sal") IntegerType else StringType,true))
    //var emp = rdd.map(_.split(",")).map(p => Row(p(0).trim.toInt,p(1),p(2).trim.toInt,p(3)))
    var emp = rdd.map(_.split(",")).map(p => Row(p(0),p(1),p(2),p(3)))
    var data = session.createDataFrame(emp,defSchema)

    data.show(2)
    data.printSchema()

    data.registerTempTable("emp")
    val d = session.sql("select name from emp order by name")
    d.show()
    //d.write.save("/home/maniram/data/empname")
   // d.write.save("/home/maniram/data/empname")
    println("Succefull")
    session.close()
//  }

}
