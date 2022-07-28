package my.exercise

import my.exercise.config.AppConfig.mainConfig
import my.exercise.util.LazyLog4j
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

class ExerciseFlow(spark: SparkSession) extends LazyLog4j {

  def run(): Unit = {
    logger.info("================================================================")
    logger.info("=====================Starting Exercise flow=====================")
    logger.info("================================================================")

    logger.info("Reading flight data files into dataframes")
    val csvReader = new CsvDataReader(spark)
    val flightDf = csvReader.read(mainConfig.input.flightDataFile).cache()
    val passengersDf = csvReader.read(mainConfig.input.passengerDataFile).cache()

    logger.info("Creating FlightDataCalc")
    val flightDataCalc: FlightDataCalc = FlightDataCalc(flightDf, passengersDf)

    import scala.collection.parallel._

    val csvWriter = new CsvDataWriter

    ParSeq(
      (() => Try(flightDataCalc.monthlyFlights()), "monthly-flights"),
      (() => Try(flightDataCalc.topNFlyers(100)), "top-100-flyers"),
      (() => Try(flightDataCalc.longestRunWithoutUK()), "longest-run-without-uk"),
      (() => Try(flightDataCalc.flownTogether(3, None, None)), "flown-together-min3"),
    ).
      foreach { case (func, location) =>
        logger.info(s"---Starting the calculation for dataset: $location")
        val tryDf = func.apply()
        tryDf match {
          case Success(df) =>
            logger.info(s"---Successfully processed the data for the dataset: $location")
            csvWriter.write(df, location)
          case Failure(e) =>
            logger.error(s"---Could not generate the data for dataset $location", e)
          //TODO: add error collection for reporting purposes?
        }
      }
    logger.info("=======End of Exercise flow=======")
  }
}
