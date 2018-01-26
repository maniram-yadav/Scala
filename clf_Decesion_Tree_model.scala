package mypc.spark.codes.ml
import org.apache.log4j.{Level,Logger}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.util.MLUtils

/**
  * Created by maniram on 25/1/18.
  */
object clf_Decesion_Tree_model {

  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("Decesion_Tree").master("local[2]").getOrCreate()
    val data =MLUtils.loadLibSVMFile(session.sparkContext,"/home/maniram/data/data/mllib/sample_libsvm_data.txt")

    val classes=2
    val catFeatureInfo=Map[Int,Int]()
    val impurity ="gini"
    val masDepth = 5
    var maxBins = 30

    val splittedData = data.randomSplit(Array(0.7,0.3))
    val (train_data,test_data)=(splittedData(0),splittedData(1))

    val clf=DecisionTree.trainClassifier(train_data,classes,catFeatureInfo,impurity,masDepth,maxBins)

    val labelPred = test_data.map{row =>
      val predictedvalue = clf.predict(row.features)
      (row.label,predictedvalue)
    }

    val testErr = labelPred.filter(row => row._1!=row._2).count().toDouble/test_data.count()


    println(s"Error in Classification : ${testErr}")
    println("Learned Classification Tree Model : \n"+clf.toDebugString)

   clf.save(session.sparkContext,"/home/maniram/data/DecesionTreereport")

  }



}
