package mypc.spark.codes.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by maniram on 21/1/18.
  */
object loadjdbcData {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]){

  val session=SparkSession.builder().appName("JDBCSQL").master("local[2]").getOrCreate()
  val sqlContext=session.sqlContext
  val data = sqlContext.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/blog").option("user","root").option("password","mysql").option("dbtable","user").load()

    data.show()
    print("Success")
  }
}
