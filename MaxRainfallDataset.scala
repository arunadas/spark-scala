package com.aruna.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Find the maximum annual rainfall by state */
object MaxRainfallDataset {

  case class Rainfall( STATE_UT_NAME:String,DISTRICT:String,ANNUAL:Double)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("MaxRainfallDataset")
      .master("local[*]")
      .getOrCreate()


    // Read the file as dataset
    import spark.implicits._
    val ds = spark.read
      .option("header",true)
      .option("inferSchema",true)
      .csv("data/districtWiseRainfallnormal.csv")
      .as[Rainfall]

    //Display 2 sample records with all columns value displayed
    ds.show(2,false)

    val maxRainfallByState = ds.groupBy("STATE_UT_NAME")
      .agg(max("ANNUAL")
        .alias("maxANNUAL"))

    val maxRain = ds.as("ds")
      .join(maxRainfallByState.as("maxData"),$"ds.STATE_UT_NAME"===$"maxData.STATE_UT_NAME"
      && $"ds.ANNUAL"===$"maxData.maxANNUAL")
      .select("ds.STATE_UT_NAME","DISTRICT","maxANNUAL")
      .orderBy(desc("maxANNUAL"))

    // Collect, format, and print the results
    val results = maxRain.collect()

    for (result <- results) {
       val state = result(0)
       val district = result(1)
       val temp = result(2).asInstanceOf[Double]
       val formattedTemp = f"$temp%.1f precipitation"
      // print formated output
      println(s"$state has $formattedTemp in district $district")
    }
  }
}