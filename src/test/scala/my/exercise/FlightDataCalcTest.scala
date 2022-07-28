package my.exercise

import my.exercise.config.AppConfig.mainConfig
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.funspec.AnyFunSpec

import java.time.LocalDate

class FlightDataCalcTest extends AnyFunSpec with SparkTestSupport {


  val csvReader = new CsvDataReader()(spark)
  val flightsDf = csvReader.read(mainConfig.input.flightDataFile)
  val passengerDf = csvReader.read(mainConfig.input.passengerDataFile)

  val flightData: FlightDataCalc = FlightDataCalc(flightsDf, passengerDf)

  describe("FlightDataCalc pre-check") {
    it("should output flight data") {
      flightData.flightsDf.filter("passengerId == '382'").show(40, false)
    }
  }

  describe("FlightDataCalc result for monthlyFlights") {
    it("should have total count and value for 2017-08 as expected") {
      val result = flightData.monthlyFlights()
      result.show(false)

      assertResult(12)(result.count) //validating total count
      assertResult(7600)(result.filter("Month == '2017-08'").select("Number of Flights").head()(0))
    }
  }

  describe("FlightDataCalc result for topNFlyers with N=100") {
    it("should have total count of 100 and Yolande Pete to be the top flyer") {
      val result = flightData.topNFlyers(100)
      result.show(false)

      assertResult(100)(result.count)
      assertResult(Seq("2068", 32, "Yolande", "Pete"))(result.head.toSeq)
    }
  }

  describe("FlightDataCalc result for longestRunWithoutUK") {
    it("should return longestRuns dataFrame as expected - checking for passengerIds in (1000, 10000, 382)") {
      val result = flightData.longestRunWithoutUK()
      result.show(false)

      result.filter(col("Longest Run").equalTo(lit(1))).show()

      //edge case - only
      assertResult(1)(result.where(col("Passenger ID").equalTo(lit("10111"))).select("Longest Run").head()(0))
      assertResult(5)(result.where(col("Passenger ID").equalTo(lit("1000"))).select("Longest Run").head()(0))
      assertResult(6)(result.where(col("Passenger ID").equalTo(lit("10000"))).select("Longest Run").head()(0))
      assertResult(11)(result.where(col("Passenger ID").equalTo(lit("382"))).select("Longest Run").head()(0))
      result.show(false)
    }
  }

  describe("FlightDataCalc for monthlyFlights") {
    it("should return flownTogether for 3 flights dataFrame for the whole date range available") {


      /*
            //For test data preparation only
            flightData.flightsDf
              .filter(col("passengerId").equalTo("210")
                .or(col("passengerId").equalTo("271")))
              .show(40, false)
            flightData.flightsDf
              .filter(col("passengerId").equalTo("2")
                .or(col("passengerId").equalTo("3")))
              .show(40, false)
      */


      val result = flightData.flownTogether(
        3,
        None,
        None
      )


      //expecting 3 common flights for passengers with Ids 210 and 271
      assertResult(3)(
        result.filter(
          col("Passenger1Id").equalTo("210")
            .and(col("Passenger2Id").equalTo("271"))
        ).select("Number of flights together").head()(0)
      )

      //There is only one common flight for passenger IDs 2 and 3, so they should not be in the output
      assert(
        result.filter(
          col("Passenger1Id").equalTo("2")
            .and(col("Passenger2Id").equalTo("3"))
        ).select("Number of flights together").isEmpty
      )

      result.show(false)

    }


    it("should return flownTogether for the date range and 2 minimum Flights as expected") {

      /*
                  //For test data preparation only
                  flightData.flightsDf
                    .filter(col("passengerId").equalTo("210")
                      .or(col("passengerId").equalTo("271")))
                    .show(40, false)
      */

      val result = flightData.flownTogether(
        2,
        Some(java.sql.Date.valueOf(LocalDate.parse("2017-01-01"))),
        Some(java.sql.Date.valueOf(LocalDate.parse("2017-01-13")))
      )

      result.show(false)

      assertResult(2)(
        result.filter(
          col("Passenger1Id").equalTo("210")
            .and(col("Passenger2Id").equalTo("271"))
        ).select("Number of flights together").head()(0)
      )

    }


  }

}
