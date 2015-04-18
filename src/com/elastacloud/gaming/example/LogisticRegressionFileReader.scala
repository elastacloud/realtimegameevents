package com.elastacloud.gaming.example

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, GeneralizedLinearModel}
import org.apache.spark.rdd.RDD

class RegressionReader(context : SparkContext) extends ReaderBase {

  var sc : SparkContext = context

  // {"time":1427846582036,"date":"2015-04-01T00:03:02.036Z","clientIp":"171.96.244.41","app":"sky","appVersion":"2.1.4.16727","event":"view","localTime":1427846511,"platform":"Android","properties":{"id":"title"},"queueDuration":0,"uid":"gf-guid.ca83882e-8141-4112-8197-40dc704cfad9"}
  def train(data : RDD[(Double, (Double, Double, Double))]) : (GeneralizedLinearModel, RDD[LabeledPoint]) = {

    // predict queue duration from client ip, local time from date parse (hour of day)
    val unscaledData = data.map { line =>
      LabeledPoint(line._1.asInstanceOf[Double], Vectors.dense(line._2._1, line._2._2, line._2._3))
    }.cache()

    (LogisticRegressionWithSGD.train(unscaledData, 100, 0.1), unscaledData)

  }

}

