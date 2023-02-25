package com.aruna.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Compute the credit card spend of India. */
object IndiaSpendAnalysisDataset {

  // Create case class with schema of CreditcardTransactions-India.csv
  case class CreditCardTx( index:Int,City: String, Date:String,CardType: String,ExpType:String, Gender:String,Amount:Int)

  /** Our main function where the action begins */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("IndiaSpendAnalysisDataset")
      .master("local[*]")
      .getOrCreate()

    // Load each line of the source data into an Dataset
    import spark.implicits._
    val ds = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/CreditcardTransactions-India.csv")
      .as[CreditCardTx]

    // credit card spend data columns extracted in dataset amtSpent
    val amtSpent = ds.select("City","Date","Gender","CardType","ExpType", "Amount")

    // Max amount spent by gender
    println("\nMax amount spent by each genders")
    amtSpent.groupBy("Gender")
      .agg(max("Amount").alias("maxAmountSpent")).show()

    println("\nTotal amount spent by each genders")
    amtSpent.groupBy("Gender")
      .agg(sum("Amount").alias("TotalAmountSpentByGender")).show()

    println("\nTotal amount spent by City")
    amtSpent.select(split($"City",",").getItem(0).alias("City")).show(5)
    amtSpent.groupBy(split($"City",",").getItem(0).alias("City"))
      .agg(sum("Amount").alias("TotalSpentByCity"))
      .orderBy(desc("TotalSpentByCity")).show()

    println("\nMax amount spent by ExpType and gender")
    amtSpent.groupBy("ExpType","Gender")
      .agg(max("Amount").alias("maxAmountSpent"))
      .orderBy("ExpType","Gender").show()

    println("\n# of transaction by CardType and gender")
    amtSpent.groupBy("CardType", "Gender")
      .count()
      .orderBy("CardType", "Gender").show()

    val data = amtSpent
      .withColumn("City",split($"City",",").getItem(0))
      .select("City","CardType","Gender")

    data.show(5)

    println("\n# of transaction by City,CardType and gender")
    data.groupBy("City","CardType", "Gender")
      .count()
      .orderBy(desc("count")).show(20)

    
    println("\n# of transaction by Date dd-MMMM-yy")
    amtSpent.groupBy(year(to_date(col("Date"),"d-MMM-yy")).alias("byYear"))
      .agg(sum("Amount")).show()

    // show max amount spend in the dataset
    amtSpent.select(max("Amount")).show()

    //Filter and where clause extraction of max amount from dataset amtSpent
    println("\n filter and where clause ")
    amtSpent.filter(amtSpent("Amount")==="998077").show()
    amtSpent.select("*").where($"Amount" === "998077").show()

    println("\n parameterize max value ")
    val min_max = amtSpent.agg(min("Amount"),max("Amount"))
    amtSpent.select("ExpType").where($"Amount" === min_max.first().get(1)).show()

  }
}
  