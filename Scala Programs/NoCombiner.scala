package com.devinline.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
object WordCount {
  def main(args: Array[String]) = {

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("NoCombiner")
      .setMaster("local")
    val sc = new SparkContext(conf)
    //load input, give the input path as command line args
    val pairRDD = sc.textFile(args(0))
    .map(line => line.split(",")).filter(fields => fields(2) =="TMAX" || fields(2) == "TMIN")
    .map(fields => ((fields(0), fields(2)), Integer.parseInt(fields(3))))
    //key is taken as (stationId, typeOfRec(i.e. either TMAX or TMIN))
    val sumByKey = pairRDD.reduceByKey((sum, temp) => temp + sum)
    val countByKey = pairRDD.foldByKey(0)((ct, temp) => ct + 1)
    val sumCountByKey = sumByKey.join(countByKey)
    val avgByKey = sumCountByKey.map(t => (t._1, t._2._1 / t._2._2.toDouble))
    //filter to TMAX or TMIN records
    val tMax = avgByKey.filter(t => t._1._2 == "TMAX").map(t => (t._1._1,t._2))
    val tMin = avgByKey.filter(t => t._1._2 == "TMIN").map(t => (t._1._1,t._2))
    //full join is performed based on stationId so that TMAX and TMIN comes in the 
    //same record
    val comb = tMax.fullOuterJoin(tMin)
    .map { case (id, (left, right)) =>
          (id, (left.getOrElse(0.00)).toString()+ ", "+ (right.getOrElse(0.00)).toString())
    }
    //save output file
   comb.saveAsTextFile("output")
  }
}

