package mypc.spark.codes.sql

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.SQLContext
import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.

/**
  * Created by maniram on 18/1/18.
  */

case class  Emp(id:Int,name:String,sal:Int,role:String)

object employee {
  Logger.getLogger("org").setLevel(Level.ERROR)

def main(args:Array[String]): Unit ={

  val conf = new SparkConf().setAppName("MySql").setMaster("local[1]")

//  val session = SparkSession.builder().appName("HousePriceSolution").master("local[1]").getOrCreate()

  val context = new SparkContext(conf)

  val sqlContext = new SQLContext(context)
  import sqlContext.implicits._



//  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

  sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, name STRING,sal INT,role String)")
  sqlContext.sql("LOAD DATA LOCAL INPATH '/home/maniram/data/emp' INTO TABLE src")
  sqlContext.sql("FROM src SELECT key, name").collect().foreach(println)






//  val data = context.textFile("/home/maniram/data/emp").map(_.split(",")).map(p => Emp(p(0).trim.toInt,p(1),p(2).trim.toInt,p(3))).toDF()
//  data.registerTempTable("emp")
//
//  val salary40 = sqlContext.sql("SELECT name,sal from emp where sal < 60000")
//  salary40.map(t => "Name : "+t(1)).collect().foreach(println)
//  salary40.map(t => "Name : "+t.getAs[String]("name")).collect().foreach(println)
//
//  salary40.show()


  //val data = session.read.json("/home/maniram/data/emp.json")
  //val mappedData = data.map(x => x.split(",")).as(Emp)
  //data foreach println

  //data.registerTempTable(data)

//  data.show()
//  println("**")
//  data.filter(data("age")<25).select(data("age")).show()
//
//  println("**")
//
//  val ageCount = data.groupBy(data.col("age"))
//  println(ageCount.count())






  //
  // data.explain()


  //val df = data.toDF()
    //df.show()
 // data.printSchema()

}
}
