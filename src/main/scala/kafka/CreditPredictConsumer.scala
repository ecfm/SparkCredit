package kafka

import kafka.serializer.StringDecoder
import ml.Credit._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Consumes messages from a topic in Kafka and enriches the message with the SVM classification
  *
  * Usage: CreditPredictConsumer  <model> <topics>
  *
  *   <model>  is the path to the saved model
  *   <topics> is a topic to consume from
  * Example:
  *    $  spark-submit --class kafka.CreditPredictConsumer --master local[2] \
  * spark-credit-1.0-SNAPSHOT.jar /user/user01/data/savemodel  /user/user01/stream:ubers /user/user01/stream:uberp
  *
  *    for more information
  *    http://maprdocs.mapr.com/home/Spark/Spark_IntegrateMapRStreams_Consume.html
  */

object CreditPredictConsumer extends Serializable {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      throw new IllegalArgumentException("You must specify the model path, subscribe topic and publish topic. For example /user/user01/data/savemodel /user/user01/stream:ubers")
    }

    val Array(modelpath , topics) = args
    System.out.println ("Use model " +  modelpath + " Subscribe to : " + topics)

    val brokers = "localhost:9092"
    val groupId = "sparkApplication"
    val batchInterval = "2"
    val pollTimeout = "10000"

    val sparkConf = new SparkConf().setAppName("CreditStream")

    val ssc = new StreamingContext(sparkConf, Seconds(batchInterval.toInt))
    val sc = ssc.sparkContext
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "smallest",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
      "spark.kafka.poll.time" -> pollTimeout,
      "spark.streaming.kafka.consumer.poll.ms" -> "8192"
    )
    // load model
    val model = SVMModel.load(sc, modelpath)

    val messagesDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet
    )
    // get message values from key,value
    val valuesDStream: DStream[String] = messagesDStream.map(_._2)

    valuesDStream.foreachRDD { rdd =>

      // There exists at least one element in RDD
      if (!rdd.isEmpty) {
        val count = rdd.count
        println("count received " + count)

        val creditDF = parseRDD(rdd).map(parseCredit).toDF()
        // Display the top 20 rows of DataFrame
        creditDF.show()
        val featureCols = Array("balance", "duration", "history", "purpose", "amount",
          "savings", "employment", "instPercent", "sexMarried", "guarantors",
          "residenceDuration", "assets", "age", "concCredit", "apartment",
          "credits", "occupation", "dependents", "hasPhone", "foreign")

        val featureIndices = creditDF.columns.union(featureCols).map(creditDF.columns.indexOf(_))
        val labelIdx = creditDF.columns.indexOf("creditability");
        val labelFeatureDF = creditDF.rdd.map(r => LabeledPoint(
          r.getDouble(labelIdx), // label
          Vectors.dense(featureIndices.map(r.getDouble(_))))) // feature
        // get cluster categories from  model
        val classAndLabels = labelFeatureDF.map { point =>
          val score = model.predict(point.features)
          val predict = if (score > 3696644) {
            1.0
          } else {
            0
          }
          (predict, point.label)
        }

        classAndLabels.toDF().show()
      }
    }

    // Start the computation
    println("start streaming")
    ssc.start()
    // Wait for the computation to terminate
    ssc.awaitTermination()

  }

}
