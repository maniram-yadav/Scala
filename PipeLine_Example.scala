package mypc.spark.codes.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
/**
  * Created by maniram on 26/1/18.
  */
object PipeLine_Example {

  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args : Array[String]): Unit ={

    val spark = SparkSession.builder().appName("PipeLine_Example").master("local[2]").getOrCreate()
    val training = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0)
    )).toDF("id", "text", "label")

    val tokenizer = new Tokenizer()
        .setInputCol("text")
        .setOutputCol("words")
    val hashingTF= new HashingTF()
          .setInputCol(tokenizer.getOutputCol)
          .setNumFeatures(700)
          .setOutputCol("features")

     val logit = new LogisticRegression()
        .setMaxIter(800)
        .setRegParam(.001)

    val pipeLine = new Pipeline()
        .setStages(Array(tokenizer,hashingTF,logit))

    val model = pipeLine.fit(dataset = training)

    pipeLine.write.overwrite().save("/home/maniram/data/PipeLine_Unfit_Model")
    model.write.overwrite().save("/home/maniram/data/PipeLine_fitted_Model")

    val model2 = PipelineModel.load("/home/maniram/data/PipeLine_fitted_Model")

    val test = spark.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "spark hadoop spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    model2.transform(test)
        .select("id","text","probability","prediction")
        .collect()
        .foreach{
          case Row(id:Long,text:String,prob: Vector ,pred:Double) =>
            println(s"($id , $text ) => prob = $prob , Prediction = $pred")
        }

  }
}
