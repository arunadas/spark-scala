package com.aruna.spark

import org.apache.log4j._
import org.apache.spark._

import scala.math.min

/** Find the minimum movie revenue */
object MinMovieRevenue {
  
  def parseLine(line:String): (Int,Double,Double,String) = {
    val fields = line.split(",")
    val year = fields(1).toInt
    val rating = fields(4).toDouble
    // handle empty strings in revenue
    val revenue = if (fields(2).isEmpty) 0 else fields(2).toDouble
   val title = fields(5)
    (year,rating,revenue,title)
  }
    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MinMovieRevenue")
    
    // Read each line of input data exclude header
    val lines = sc.textFile("data/IMDB-Movie-Data.csv")
      .mapPartitionsWithIndex{(index,iter) => if(index == 0) iter.drop(1) else iter}
    
    // Convert to (stationID, entryType, temperature) tuples
    val parsedLines = lines.map(parseLine)

    // pack revenue key with title and rating
    val moviesWithTitle = parsedLines.map(x=> (x._3 ,(x._4,x._2)))


     // Convert to (year, revenue)
       val yearRevenue = parsedLines.map(x => (x._1, x._3))

    // Reduce by year retaining the max revenue found
    val minRevenueByYear = yearRevenue.reduceByKey( (x,y) => min(x,y))

    // flipped to get min revenue as key
    val flipped = minRevenueByYear.map(x=> (x._2,x._1))

    // joined on min revenue by year
    val joinedMinRevenue = flipped.join(moviesWithTitle)
    //flipped to year for sorting
    val flipByYear = joinedMinRevenue.map(x=>(x._2,x._1))

    // Collect, format, and print the results
    val results = flipByYear.collect()

    for (result <- results.sorted) {
       val moviewithRating = result._1
       val year = moviewithRating._1
       val titleRating = moviewithRating._2
       val title = titleRating._1
       val rating = titleRating._2
       val temp = result._2
       val formattedTemp = f"$temp%.1f million"
      //missing values give incorrect results because of empty string
      println(s"$title made $formattedTemp in $year with rating $rating")
    }
      
  }
}