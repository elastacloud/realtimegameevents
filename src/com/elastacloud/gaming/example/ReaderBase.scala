package com.elastacloud.gaming.example

import org.apache.spark.mllib.regression.{LabeledPoint, GeneralizedLinearModel}
import org.apache.spark.rdd.RDD

trait ReaderBase {
  case class ParseOp[T](op: String => T)
  implicit val toDouble = ParseOp[Double](_.toDouble)
  def parse[T: ParseOp](s: String) = implicitly[ParseOp[T]].op(s)

  def train(data : RDD[(Double, (Double, Double, Double))]) : (GeneralizedLinearModel, RDD[LabeledPoint])
}
