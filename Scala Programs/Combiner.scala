package com.devinline.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object Combiner {
  def main(args: Array[String]) = {

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("NoCombiner")
      .setMaster("local")
    val sc = new SparkContext(conf)
    //load input, give the input path as command line args
    val pairRDD = sc.textFile(args(0))
      .map(line => line.split(",")).filter(fields => fields(2) == "TMAX" || fields(2) == "TMIN")
      .map(fields => ((fields(0), fields(2)), Integer.parseInt(fields(3))))
    //combine by key acts like a combiner
    //it performs three steps, createcombiner, mergevalue and mergecombiner
    //finally we get sum and count for each stationId in the single step of combineByKey
    val sumCountByKey = pairRDD.combineByKey(
      value => (value, 1),
      (acc1: (Int, Int), temp) => (acc1._1 + temp, acc1._2 + 1),
      (acc2: (Int, Int), acc3: (Int, Int)) => (acc2._1 + acc3._1, acc2._2 + acc3._2))
    val avgByKey = sumCountByKey.map(t => (t._1, t._2._1 / t._2._2.toDouble))
    val tMax = avgByKey.filter(t => t._1._2 == "TMAX").map(t => (t._1._1, t._2))
    val tMin = avgByKey.filter(t => t._1._2 == "TMIN").map(t => (t._1._1, t._2))
    val comb = tMin.fullOuterJoin(tMax)
      .map {
        case (id, (left, right)) =>
          (id, (left.getOrElse(0.00)).toString() + ", " + (right.getOrElse(0.00)).toString())
      }
     comb.saveAsTextFile("output")
  }
}