package com.elastacloud.gaming.example

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.GeneralizedLinearModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.messaging.AzureUtils
import org.apache.spark.streaming.messaging.servicebus.AzureServiceBusSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Richard on 4/15/2015.
 */
object SQLContextSingleton {
  @transient private var instance: SQLContext = null

  // Instantiate SQLContext on demand
  def getInstance(sparkContext: SparkContext): SQLContext = synchronized {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}

object ServiceBusConfiguration {
  // Set the defaults including the Sas and other pieces of information
  val sas = "sr=https%3a%2f%2fsparkstreaming.servicebus.windows.net%2fgamingevents&sig=DZlWZUw7GGSFqePPVm8HQ%2fvdCPlbrHMk6zgXwtTpL%2bo%3d&se=1457535234&skn=sparkpol"
  val subscriptionName = "spark-livedata"
  val topicName = "gamingevents"
  val namespace = "sparkstreaming"
  val sasOut = "sr=https%3a%2f%2fsparkstreaming.servicebus.windows.net%2fgamingout&sig=YPbqsKOmbmLBTZPDHz7j6EJAreCygX0ofU%2bhtJP8Phs%3d&se=1461079802&skn=sparkpol"
  val topicNameOut = "gamingout"
}

class ConfigureSteps {

  var isClustered = true

  def createSparkStreaming(args : Array[String]) : StreamingContext = {

    // Always do for local to ensure that the Hadoop tools are set correctly
    if (args(0).contains("local")) {
      System.setProperty("hadoop.home.dir", args(1))
      isClustered = false
    }
    // Set the Spark conf value which will tune the TTL value for streaming
    val conf = new SparkConf()
      .setAppName("Future value predictor")
      .setMaster(args(0))
      .set("spark.cleaner.ttl", "100000")

    // Set up the checkpoint for streamingcontext
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("./checkpoint")
    ssc
  }

  def createAzureStream(ssc : StreamingContext) : ReceiverInputDStream[String] = {
    // Create the service bus session with a 30 second checkpoint
    val sender = new AzureServiceBusSession(ServiceBusConfiguration.namespace, ServiceBusConfiguration.topicName, Some(ServiceBusConfiguration.subscriptionName), ServiceBusConfiguration.sas)
    val gamingReader = AzureUtils.createStream(ssc, sender, Nil, StorageLevel.MEMORY_ONLY_SER_2)
    gamingReader.checkpoint(Seconds(30))
    gamingReader
  }

  def trainModel(ssc : StreamingContext) = {

    val sqlContext = SQLContextSingleton.getInstance(ssc.sparkContext)
    val location = isClustered match {
      case true => "/gamingin/*.json"
      case _ => "D:\\Projects\\data"
    }
    val jsonFile = sqlContext.jsonFile(location).toDF()
    jsonFile.registerTempTable("gameevents")
    val query = sqlContext.sql("select Message, LevelTimeRemaining, LevelScore, LevelCompleteness from gameevents")

    val playerData = query.map { line =>
      val label = line(0) match {
        case "Monetize" => 1D
        case _ => 0D
      }
      val levelTimeRemaining = line(1)
      val levelScore = line(2)
      val levelCompleteness = line(3)
      (label, (levelTimeRemaining.asInstanceOf[Double], levelScore.asInstanceOf[Long].toDouble, levelCompleteness.asInstanceOf[Double]))
    }
    val splits = playerData.randomSplit(Array(0.7, 0.3), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Train a Logit model
    val reader = new RegressionReader(ssc.sparkContext)
    (reader.train(playerData), test)
  }

  def processMesssages(gamingReader : ReceiverInputDStream[String], model : GeneralizedLinearModel) = {

    // enumerate RDDs so that you can enable the streaming capability

    gamingReader.foreachRDD { rdd =>

      // Get the singleton instance of SQLContext
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)

      val json = sqlContext.jsonRDD(rdd)

      // Convert RDD[String] to RDD[case class] to DataFrame
      val jsonDataFrame = json.toDF()

      jsonDataFrame.printSchema()

      // Register as table
      jsonDataFrame.registerTempTable("gameevents")

      // Do word count on DataFrame using SQL and print it
      val uidDataFrame =
        sqlContext.sql("select Message, LevelTimeRemaining, LevelScore, LevelCompleteness, GameId, GamerId from gameevents where not (Message='Monetize' or Message='MonetizeDeclined')")
      uidDataFrame.show()
      val vectorInput = uidDataFrame.map { row =>
        val levelTimeRemaining = row(1).asInstanceOf[Double]
        val levelScore = row(2).asInstanceOf[Long].toDouble
        val levelCompleteness = row(3).asInstanceOf[Double]
        val gameId = row(4)
        val gamerId = row(5)
        (gameId, gamerId, Vectors.dense(levelTimeRemaining, levelScore, levelCompleteness))
      }

      val sender = new AzureServiceBusSession(ServiceBusConfiguration.namespace, ServiceBusConfiguration.topicNameOut, Some(ServiceBusConfiguration.subscriptionName), ServiceBusConfiguration.sasOut)

      vectorInput.foreach { input =>
        val prediction = model.predict(input._3)
        sender.send("{\"GameId\":\"%s\",\"GamerId\":\"%s\",\"ShouldMonetize\":%f}".format(input._1, input._2, prediction))
      }
    }
  }

  def printModelAUC(model : GeneralizedLinearModel, labels : RDD[(Double, (Double, Double, Double))]) = {

    val scoreAndLabels = labels.map { point =>
      val score = model.predict(Vectors.dense(point._2._1, point._2._2, point._2._3))
      (score, point._1)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    println("AUC: " + metrics.areaUnderROC())

  }
}

object Main {

  def main(args : Array[String]): Unit = {

    val steps = new ConfigureSteps()
    val ssc = steps.createSparkStreaming(args)
    val gamingReader = steps.createAzureStream(ssc)
    val model = steps.trainModel(ssc)
    steps.printModelAUC(model._1._1, model._2)

    steps.processMesssages(gamingReader, model._1._1)

    gamingReader.print()

    ssc.start()
    ssc.awaitTermination()
  }


}