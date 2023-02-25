package com.aruna.spark

import org.apache.log4j._
import org.apache.spark._

import scala.math.max

/** Find the maximum rainfall by state for a year */
object MaxRainfall {
  
  def parseLine(line:String): (String, String, String) = {
    val fields = line.split(",")
    val STATE_UT_NAME = fields(0)
    val DISTRICT = fields(1)
    val ANNUAL = fields(14)
    (STATE_UT_NAME, DISTRICT, ANNUAL)
  }
    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MaxRainfall")
    
    val lines = sc.textFile("data/districtWiseRainfallnormal.csv")
    val parsedLines = lines.map(parseLine)
      .mapPartitionsWithIndex{(index,iter) => if (index == 0) iter.drop(1) else iter}

     val stateRainfall = parsedLines.map(x => (x._1, x._3.toFloat))
     val maxRainByState = stateRainfall.reduceByKey( (x,y) => max(x,y))
     val results = maxRainByState.collect()

     for (result <- results.sorted) {
       val state = result._1
       val temp = result._2
       val formattedRain = f"$temp%.1f "
       println(s"$state max annual rainfall: $formattedRain")
    }
      
  }
}