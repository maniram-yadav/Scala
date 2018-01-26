package mypc.spark.codes.ml
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{Row,SparkSession}
import org.apache.log4j.{Logger,Level}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector,Vectors}


/**
  * Created by maniram on 26/1/18.
  */
object Logitregression_with_mlpack {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]): Unit ={

    val spark =SparkSession.builder().appName("Logistics_Classification_withmlpack").master("local[2]").getOrCreate()
    val training_data = spark.createDataFrame(Seq(

      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
      (1.0, Vectors.dense(0.0, 1.2, -0.5)),
      (0.0, Vectors.dense(2.0, 1.5, 1.0)),
      (1.0, Vectors.dense(0.0, 1.3, 1.0))
    )).toDF("label","features")

    val lr = new LogisticRegression()

    println("LogisticRegression Parameter : "+lr.explainParams())
    lr.setMaxIter(20)
        .setRegParam(.02)

    val model = lr.fit(training_data)
    println(s"Model has been fitted using parameter :  ${model.parent.extractParamMap}")
    println(s"Model has been fitted using parameter :  ${model.parent.explainParams()}")




    val paramMap =  ParamMap(lr.maxIter->20)
        .put(lr.maxIter,20)
        .put(lr.regParam->.02,lr.regParam->.1)
    val paramMap2 = ParamMap(lr.probabilityCol-> "MyProbablty")
    val combinedparam = paramMap ++ paramMap2
    val model2 = lr.fit(training_data,combinedparam)
    println("#"*150+"\n"*3)
    println("Logistics regression Parameter Detail  :  "+model2.extractParamMap)

    println("#"*100+"\n"*3)
    val test_data = spark.createDataFrame(Seq(
      (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
      (0.0, Vectors.dense(3.0, 2.0, -0.1)),
      (1.0, Vectors.dense(0.0, 2.2, -1.5))
    )).toDF("label", "features")
    model2.transform(test_data)
          .select("features","label","MyProbablty","prediction")
          .collect()
          .foreach { case Row(features: Vector, label: Double, prob : Vector, prediction : Double ) =>
            println(s"( $features , $label )  =>   prob => $prob , prediction => $prediction ")
          }

  }

}
