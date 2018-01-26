package mypc.spark.codes.ml
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * Created by maniram on 25/1/18.
  */
object MultiClassClassification {


  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Regression").master("local[2]").getOrCreate()

    val data = MLUtils.loadLibSVMFile(spark.sparkContext,"/home/maniram/data/data/mllib/sample_multiclass_classification_data.txt")

    val Array(train,test) = data.randomSplit(Array(.7,.3),seed=2L)
    val logreg = new LogisticRegressionWithLBFGS()
        .setNumClasses(3)
        .run(train)

    val testPredictLevel = test.map{ case LabeledPoint(label,feature) =>
      val prediction=logreg.predict(feature)
      (prediction,label)
    }

    val metrics = new MulticlassMetrics(testPredictLevel)

    println("Accuracy : "+metrics.accuracy)
    println("Confusion Matrix : "+metrics.confusionMatrix)
    val lebels = metrics.labels

    lebels.foreach(l =>
    println(s"Precesion : ${l}  " + metrics.precision(l))
    )


    lebels.foreach(l =>
      println(s"Recall : ${l}  " + metrics.recall(l))
    )

    lebels.foreach(l =>
      println(s"False Positive : ${l}  " + metrics.falsePositiveRate(l))
    )

    lebels.foreach(l =>
      println(s"F Measure : ${l}  " + metrics.fMeasure(l))
    )

    println(s"Weighted false Positive :  " + metrics.weightedFalsePositiveRate)
    println(s"Weight F Measure :  " + metrics.weightedFMeasure)
    println(s"Weighted Recall :  " + metrics.weightedRecall)
    println(s"Weighted Precision  " + metrics.weightedPrecision)
  spark.stop()
  }

}
