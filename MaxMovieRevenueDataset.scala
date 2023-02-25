package com.aruna.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}

/** Find the maximum movie revenue  */
object MaxMovieRevenueDataset {

  case class Movie(rank:Int,  year: Int, Revenue_millions: Double,RuntimeMinutes:Int,
                   Rating:Double,Title: String, Genre:String,Director:String,Actors:String,
                   Votes:Int, Metascore:Int)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("MaxMovieRevenueDataset")
      .master("local[*]")
      .getOrCreate()


    // Read the file as dataset
    import spark.implicits._
    val ds = spark.read
      .option("header",true)
      .option("inferSchema",true)
      .csv("data/IMDB-Movie-Data.csv")
      .as[Movie]

    //Display 2 sample records with all columns value displayed
    ds.show(2,false)

    val maxRevenueByYear = ds.groupBy("Year")
      .agg(max("Revenue_millions")
        .alias("maxRevenueInMillions"))

    // Filter out all but TMAX entries
    val maxRevenue = ds.as("ds")
      .join(maxRevenueByYear.as("maxData"),$"ds.Year"===$"maxData.Year"
      && $"ds.Revenue_millions"===$"maxData.maxRevenueInMillions")
      .select("ds.Year","Revenue_millions","Rating","Title")
      .orderBy("Year")

    // Collect, format, and print the results
    val results = maxRevenue.collect()

    for (result <- results) {
       val year = result(0)
       val temp = result(1).asInstanceOf[Double]
       val formattedTemp = f"$temp%.1f million"
       val rating = result(2)
       val title = result(3)
      // print formated output
      println(s"$title made $formattedTemp in $year with rating $rating")
    }
  }
}