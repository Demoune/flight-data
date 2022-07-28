package my.exercise

import my.exercise.config.ExerciseConfig
import pureconfig._
import pureconfig.generic.auto._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.expr

import java.util.Date

/*
Main application class, responsible for configuration and initializing sparkSession, and colling the actual logic
 */
object ExerciseMain extends App with SparkSupport {

/*  val config = ConfigSource.default.load[ExerciseConfig] match {
    case Left(configReaderFailures) =>
      sys.error(s"Encountered the following errors reading the configuration: ${configReaderFailures.toList.mkString("\n")}")
    case Right(config) =>
      println(s"Input: flightDataFile: ${config.input.flightDataFile}")
      println(s"Input: passengerDataFile: ${config.input.passengerDataFile}")
      println(s"Output: basePath:  ${config.output.basePath}")*/


/*
  val spark = SparkSession.builder
    .appName("Simple Application")
    .config("spark.master", "local")
    .getOrCreate()
*/

  val flow: ExerciseFlow = new ExerciseFlow(spark)
  flow.run()

  //flightDf.join(passengersDf, usingColumn = "_c0", joinType = "left").withColumn("numberOfFlights", expr())



//  val numAs = logData.filter(line => line.contains("a")).count()
//  val numBs = logData.filter(line => line.contains("b")).count()


  //println(s"Lines with a: $numAs, Lines with b: $numBs")
  spark.stop()

}

