package my.exercise

import my.exercise.config.AppConfig.mainConfig
import org.apache.spark.sql.SparkSession

class ExerciseFlow(spark: SparkSession) {

  def run(): Unit = {
    val flightPath = mainConfig.input.flightDataFile
    val passengerPath = mainConfig.input.passengerDataFile

    val outputBase = "output/"
    val flightDf = spark.read.csv(flightPath).cache()
    val passengersDf = spark.read.csv(passengerPath).cache()

    flightDf.printSchema()

    passengersDf.printSchema()

    println("First 20 records:")
    flightDf.groupBy("_c4").count.show(20, false)
    flightDf.groupBy("_c4").count.write.csv(outputBase + "flightsByYear")
  }
}
