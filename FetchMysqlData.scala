package mypc.spark.codes.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
/**
  *    My MySqlSchema
  *
  * id: integer
  * email: string
  * firstname: string
  * lastname: string
  * userid: string
  * password: string
  * verified: boolean
  *
  *
  *
  * Created by maniram on 25/1/18.
  */


object FetchMysqlData {

  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val session = SparkSession.builder().appName("MySqldata").master("local[2]").getOrCreate()
     val data = session.read.format("jdbc")
        .option("url","jdbc:mysql://localhost:3306/blog")
        .option("user","root")
        .option("password","mysql")
        .option("dbtable","user")
        .load()
    data.show()
    data.printSchema()

    data.select("email","userid","password").show()
    session.stop()
  }

}
