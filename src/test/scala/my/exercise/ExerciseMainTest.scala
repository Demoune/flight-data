package my.exercise

import my.exercise.config.AppConfig.mainConfig
import org.scalatest.funspec.AnyFunSpec


class ExerciseMainTest extends AnyFunSpec with SparkTestSupport {

  describe("") {

    val csvReader = new CsvDataReader()(spark)
//    mainConfig.input
//    val flightsDf = csvReader.read(mainConfig.input.flightDataFile)
//    val passengerDf = csvReader.read(mainConfig.input.passengerDataFile)
//
//    val flightData = FlightData()
    it("should ") {

    }

  }



}
