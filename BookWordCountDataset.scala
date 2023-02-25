package com.aruna.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Count up how many of each word occurs in a book, using regular expressions. */
object BookWordCountDataset {

  case class Book(value: String)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("BookWordCountDataset")
      .master("local[*]")
      .getOrCreate()

    // Read each line of my book into an Dataset
    import spark.implicits._
    val warChiefinput = spark.read.text("data/TheWarChief.txt").as[Book]

    // Split using a regular expression that extracts words
    val warWords = warChiefinput
      .select(explode(split($"value", "\\W+")).alias("word"))

    // Normalize everything to lowercase
    val lowercaseWords = warWords.select(lower($"word").alias("word"))

    // Count up the occurrences of each word
    val warwordCounts = lowercaseWords.groupBy("word").count().orderBy(desc("count"))

    // Read each line of my book into an Dataset
    import spark.implicits._
    val RJinput = spark.read.text("data/RomeoandJuliet.txt").as[Book]

    // Split using a regular expression that extracts words
    val rjWords = RJinput
      .select(explode(split($"value", "\\W+")).alias("word"))

    // Normalize everything to lowercase
    val lowercaseRJWords = rjWords.select(lower($"word").alias("word"))

    // Count up the occurrences of each word
    val rjwordCounts = lowercaseRJWords.groupBy("word").count().orderBy(desc("count"))

    // Read each line of my book into an Dataset
    import spark.implicits._
    val alchemistinput = spark.read.text("data/TheAlchemist.txt").as[Book]

    // Split using a regular expression that extracts words
    val acWords = alchemistinput
      .select(explode(split($"value", "\\W+")).alias("word"))

    // Normalize everything to lowercase
    val lowercaseACWords = acWords.select(regexp_replace(lower($"word"),"\\s+","").alias("word"))

    // Count up the occurrences of each word
    val wordCounts = lowercaseACWords.groupBy("word").count().orderBy(desc("count"))

    // Show the results.
    println("Top 20 words in The war Chief")
    warwordCounts.show(20)

    println("Top 20 words in The Romeo Juliet")
    rjwordCounts.show(20)

    println("Top 20 words in The Alchemist")
    wordCounts.show(20)
  }
}

