package mypc.spark.codes.ml
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.regression.GeneralizedLinearRegression

/**
  * Created by maniram on 23/1/18.
  */
object LinRegElastinet {

  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Regression").master("local[2]").getOrCreate()
    val data = spark.read.format("libsvm").load("/home/maniram/big_data/spark/data/mllib/sample_linear_regression_data.txt")

    val lr = new LinearRegression()
        .setMaxIter(10)
        .setRegParam(0.3)
        .setElasticNetParam(.8)

    val lrmodel = lr.fit(data)

    println(s"Coefficients : ${lrmodel.coefficients}  Intercept : ${lrmodel.intercept}")
    println("*"*100)


    val glr =new GeneralizedLinearRegression()
        .setFamily("gaussian")
        .setLink("identity")
        .setMaxIter(15)
        .setRegParam(.8)

    val glrmodel = glr.fit(data)

    println(s"Coefficients : ${glrmodel.coefficients}  Intercept : ${glrmodel.intercept}")
    val summary=glrmodel.summary
    println(s"Rank :  ${summary.rank}")
    println(s"MSE : ${summary.coefficientStandardErrors}")
    println(s"Deviance :  ${summary.deviance}")

    println("*"*100)




  }


}
