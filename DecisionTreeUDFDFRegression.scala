package com.aruna.spark

import org.apache.log4j._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col,udf}


object DecisionTreeUDFDFRegression {
  
  case class RegressionSchema(longitude: Double,	latitude: Double,	housing_median_age:Double,
      total_rooms:Double,	total_bedrooms:Double,	population:Double,	households:Double,
    median_income:Double,	median_house_value:Double,	ocean_proximity: String)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("DecisionTreeUDFDFRegression")
      .master("local[*]")
      .getOrCreate()


    import spark.implicits._
    val dsRaw = spark.read
      .option("sep", ",")
      .option("header",true)
      .option("inferSchema",true)
      .csv("data/california_housing.csv")
      .as[RegressionSchema]
    //dsRaw.show(2)

    // We start by declaring an "anonymous function" in Scala
    val ocean_proximity = udf((ocean_proximity:String) =>
    if (ocean_proximity == "NEAR BAY") 1 else 0
    )

    val udfdfRaw = dsRaw.withColumn("ocean_proximity", ocean_proximity(col("ocean_proximity")))
      udfdfRaw.show(2)


    val assembler = new VectorAssembler().
      setInputCols(Array("housing_median_age","total_rooms","total_bedrooms","population",
      "households","median_income","ocean_proximity")).
      setOutputCol("features")

    //setHandleInvalid to handle empty and null values
    val df = assembler.setHandleInvalid("skip").transform(udfdfRaw)
        .select("median_house_value","features")

    // Let's split our data into training data and testing data
    val trainTest = df.randomSplit(Array(0.5, 0.5))
    val trainingDF = trainTest(0)
    val testDF = trainTest(1)

    // Now create our linear regression model
    val dtr = new DecisionTreeRegressor()
      .setFeaturesCol("features")
      .setLabelCol("median_house_value")

    // Train the model using our training data
    val model = dtr.fit(trainingDF)

    // Now see if we can predict values in our test data.
    // Generate predictions using our decision Tree regression model for all features in our
    // test dataframe:
    val fullPredictions = model.transform(testDF).cache()

    // This basically adds a "prediction" column to our testDF dataframe.

    // Extract the predictions and the "known" correct labels.
    val predictionAndLabel = fullPredictions.select("prediction", "median_house_value").collect()


    // Print out the predicted and actual values for each point

    for (prediction <- predictionAndLabel) {

      println(prediction)

    }

    // Stop the session
    spark.stop()

  }
}