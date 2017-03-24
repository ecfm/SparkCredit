package ml

import org.apache.spark._
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

object Credit {

  case class Credit(
                     creditability: Double,
                     balance: Double, duration: Double, history: Double, purpose: Double, amount: Double,
                     savings: Double, employment: Double, instPercent: Double, sexMarried: Double, guarantors: Double,
                     residenceDuration: Double, assets: Double, age: Double, concCredit: Double, apartment: Double,
                     credits: Double, occupation: Double, dependents: Double, hasPhone: Double, foreign: Double
                   )

  def parseCredit(line: Array[Double]): Credit = {
    Credit(
      line(0),
      line(1) - 1, line(2), line(3), line(4), line(5),
      line(6) - 1, line(7) - 1, line(8), line(9) - 1, line(10) - 1,
      line(11) - 1, line(12) - 1, line(13), line(14) - 1, line(15) - 1,
      line(16) - 1, line(17) - 1, line(18) - 1, line(19) - 1, line(20) - 1
    )
  }

  def parseRDD(rdd: RDD[String]): RDD[Array[Double]] = {
    rdd.map(_.split(",")) // convert every line to array of String fields
      .map(stringFields => stringFields.map(field => field.toDouble)) // convert every String field to Double
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SparkDFebay")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val creditDF = parseRDD(sc.textFile("/Users/Chengfeng/Cask/SparkCredit/data/german_credit.csv"))
      .map(parseCredit).toDF().cache()
//    creditDF.registerTempTable("credit")
//    creditDF.printSchema

//    creditDF.show

//    sqlContext.sql("SELECT creditability, avg(balance) as avgbalance, avg(amount) as avgamt, avg(duration) as avgdur  FROM credit GROUP BY creditability ").show

//    creditDF.describe("balance").show
//    creditDF.groupBy("creditability").avg("balance").show

    val featureCols = Array("balance", "duration", "history", "purpose", "amount",
      "savings", "employment", "instPercent", "sexMarried", "guarantors",
      "residenceDuration", "assets", "age", "concCredit", "apartment",
      "credits", "occupation", "dependents", "hasPhone", "foreign")

    val featureIndices = creditDF.columns.union(featureCols).map(creditDF.columns.indexOf(_))
    val labelIdx = creditDF.columns.indexOf("creditability");
    val labelFeatureDF = creditDF.rdd.map(r => LabeledPoint(
      r.getDouble(labelIdx), // label
      Vectors.dense(featureIndices.map(r.getDouble(_))))) // feature
    val splitSeed = 5043
    val Array(trainingData, testData) = labelFeatureDF.randomSplit(Array(0.7, 0.3), splitSeed)
    val numIterations = 20
    val model = SVMWithSGD.train(trainingData, numIterations, 1, 1.0)
    model.save(sc, "/Users/Chengfeng/Cask/SparkCredit/model/svm_model")

    // Clear the default threshold to output raw scores
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = testData.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    val classAndLabels = scoreAndLabels.map { scoreLabel =>
        val predict = if (scoreLabel._1 > 3696644) {
          1.0
        } else {
          0
        }
      (predict, scoreLabel._2)
    }
//    scoreAndLabels.filter(_._2 < 1.0).toDF().show()
//    scoreAndLabels.filter(_._2 == 1.0).toDF().show()
//    // Get evaluation metrics.
//    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
//    metrics.thresholds().sortBy(v => v, false).toDS().show(30)
//    metrics.roc().sortBy(_._2, false).toDF().show(30)
//    val auROC = metrics.areaUnderROC()
//    println("Area under ROC = " + auROC)
    val multiMetrics = new MulticlassMetrics(classAndLabels)
    println(multiMetrics.confusionMatrix)



    // ================ Random Forest =================
//    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
//    val featureDF = assembler.transform(creditDF)
//    featureDF.show
//
//    val labelIndexer = new StringIndexer().setInputCol("creditability").setOutputCol("label")
//    val featureLabelDF = labelIndexer.fit(featureDF).transform(featureDF)
//    featureLabelDF.show
//    val splitSeed = 5043
//    val Array(trainingData, testData) = featureLabelDF.randomSplit(Array(0.7, 0.3), splitSeed)
//
//    val classifier = new RandomForestClassifier()
//      .setImpurity("gini")
//      .setMaxDepth(3)
//      .setNumTrees(20)
//      .setFeatureSubsetStrategy("auto")
//      .setSeed(5043)
//    val model = classifier.fit(trainingData)
//
//    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label")
//    val predictions = model.transform(testData)
////    model.toDebugString
//
//    val accuracy = evaluator.evaluate(predictions)
//    println("accuracy before pipeline fitting: " + accuracy)
//
//    val rm = new RegressionMetrics(
//      predictions.select("prediction", "label").rdd.map(x =>
//        (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double]))
//    )
//    println("MSE: " + rm.meanSquaredError)
//    println("MAE: " + rm.meanAbsoluteError)
//    println("RMSE Squared: " + rm.rootMeanSquaredError)
//    println("R Squared: " + rm.r2)
//    println("Explained Variance: " + rm.explainedVariance + "\n")

// =========== use Param Grid to search for the best random forest ============

//    val paramGrid = new ParamGridBuilder()
//      .addGrid(classifier.maxBins, Array(25, 31))
//      .addGrid(classifier.maxDepth, Array(5, 10))
//      .addGrid(classifier.numTrees, Array(20, 60))
//      .addGrid(classifier.impurity, Array("entropy", "gini"))
//      .build()
//
//    val steps: Array[PipelineStage] = Array(classifier)
//    val pipeline = new Pipeline().setStages(steps)
//
//    val cv = new CrossValidator()
//      .setEstimator(pipeline)
//      .setEvaluator(evaluator)
//      .setEstimatorParamMaps(paramGrid)
//      .setNumFolds(10)
//
//    val pipelineFittedModel = cv.fit(trainingData)
//
//    val predictions2 = pipelineFittedModel.transform(testData)
//    val accuracy2 = evaluator.evaluate(predictions2)
//    println("accuracy after pipeline fitting" + accuracy2)
//
//    println(pipelineFittedModel.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel].stages(0))
//
//    pipelineFittedModel
//      .bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel]
//      .stages(0)
//      .extractParamMap
//
//    val rm2 = new RegressionMetrics(
//      predictions2.select("prediction", "label").rdd.map(x =>
//        (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double]))
//    )
//
//    println("MSE: " + rm2.meanSquaredError)
//    println("MAE: " + rm2.meanAbsoluteError)
//    println("RMSE Squared: " + rm2.rootMeanSquaredError)
//    println("R Squared: " + rm2.r2)
//    println("Explained Variance: " + rm2.explainedVariance + "\n")
  }
}
