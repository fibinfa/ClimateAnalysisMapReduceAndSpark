package com.devinline.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.tools.nsc.dependencies.Files
import java.nio.file.Files

object SecondarySort {
  def main(args: Array[String]) = {

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("SecondarySort")
      .setMaster("local")
    val sc = new SparkContext(conf)
    //load all txt files(can input multiple files)
    val pairRDD = sc.textFile("*.txt")
      .map(line => line.split(",")).filter(fields => fields(2) == "TMAX" || fields(2) == "TMIN")
      .map(fields => ((fields(0), fields(1).substring(0, 4), fields(2)), Integer.parseInt(fields(3))))
    //    pairRDD.foreach(println)
    val sumByKey = pairRDD.reduceByKey((sum, temp) => temp + sum)
    val countByKey = pairRDD.foldByKey(0)((ct, temp) => ct + 1)
    val sumCountByKey = sumByKey.join(countByKey)
    val avgByKey = sumCountByKey.map(t => (t._1, t._2._1 / t._2._2.toDouble))
    val tMax = avgByKey.filter(t => t._1._3 == "TMAX").map(t => ((t._1._1, t._1._2), t._2))
    val tMin = avgByKey.filter(t => t._1._3 == "TMIN").map(t => ((t._1._1, t._1._2), t._2))
    val comb = tMax.fullOuterJoin(tMin)
      .map {
        case (id, (left, right)) =>
          (id, (left.getOrElse(0.00)).toString() + ", " + (right.getOrElse(0.00)).toString())
      }
    //    comb.foreach(println)

    val interOut = comb.map(f => ((f._1._1), (f._1._2, f._2)))
    //it groups all the records based on stationId
    val groupedOut = interOut.groupByKey()
    //output is first sorted based on stationId and then on year
    val sortedOut = groupedOut.sortByKey().sortBy(_._1, ascending = true)

    sortedOut.saveAsTextFile("output")
  }
}