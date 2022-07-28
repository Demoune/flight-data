package my.exercise

import my.exercise.config.AppConfig.mainConfig
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.funspec.AnyFunSpec

class FlightDataTest extends AnyFunSpec with SparkTestSupport {


  describe("FlightData constructed from test csv files") {

    val csvReader = new CsvDataReader()(spark)
    val flightsDf = csvReader.read(mainConfig.input.flightDataFile)
    val passengerDf = csvReader.read(mainConfig.input.passengerDataFile)

    val flightData: FlightData = FlightData(flightsDf, passengerDf)

    it("should output flight data") {
      flightData.flightsDf.where("passengerId == '382'").show(100,false)

    }

    it("should return monthlyFlights dataFrame having 343 rows and matching value for 2017-08-03") {
      val result = flightData.monthlyFlights()
      val count = result.count

      assertResult(343)(count)  //validating total count
      assertResult(200)(result.where("date == '2017-08-03'").select("count").head()(0))
      result.show(false)
    }

    it("should return top 100 Flyers dataFrame and the first is matching") {
      val result = flightData.topNFlyers(100)
      assertResult(100)(result.count)
      assertResult(Seq("2068", 32, "Yolande", "Pete"))(result.head.toSeq)
      result.show(false)
    }

    it("should return longestRuns dataFrame as expected - checking for passengerIds in (1000, 10000, 382)") {
      val result = flightData.longestRunWithoutUK()
      //val longestRunFor45 = result.where("'Passenger ID' == 45").select("Longest Run").head()(0)
      //assertResult(9)(longestRunFor45)
      result.show(false)
      assertResult(5)(result.where(col("Passenger ID").equalTo(lit("1000"))).select("Longest Run").head()(0))
      assertResult(6)(result.where(col("Passenger ID").equalTo(lit("10000"))).select("Longest Run").head()(0))
      assertResult(11)(result.where(col("Passenger ID").equalTo(lit("382"))).select("Longest Run").head()(0))
      result.show(false)
    }

    it("should return flownTogether for 3 flights dataFrame as expected") {



    }



  }

}
