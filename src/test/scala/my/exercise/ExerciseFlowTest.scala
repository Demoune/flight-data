package my.exercise

import my.exercise.config.AppConfig.mainConfig
import my.exercise.utils.{SparkTestSupport, TestUtils}
import org.scalatest.BeforeAndAfter
import org.scalatest.funspec.AnyFunSpec

import java.io.File
import java.nio.file.{Path, Paths}

class ExerciseFlowTest extends AnyFunSpec with SparkTestSupport with BeforeAndAfter {

  before {
    val rootFolder = new File(mainConfig.output.basePath)
    if (rootFolder.exists()) {
      TestUtils.remove(Paths.get(mainConfig.output.basePath))
    }
  }

  describe("ExerciseFlow run") {
    it("should successfully read 2 input files and generate 4 csv files in output locations") {
      val exerciseFlow = new ExerciseFlow(spark)
      exerciseFlow.run()

      val csvReader = new CsvDataReader(spark)
      val flownTogetherDf = csvReader.read(mainConfig.output.basePath + "/" + "flown-together-min3" )
      val longestRunDf = csvReader.read(mainConfig.output.basePath + "/" + "longest-run-without-uk" )
      val monthlyFlightsDf = csvReader.read(mainConfig.output.basePath + "/" + "monthly-flights" )
      val top100FlyersDf = csvReader.read(mainConfig.output.basePath + "/" + "top-100-flyers" )

        assertResult(12)(monthlyFlightsDf.count()) // 1 year data
        assertResult(100)(top100FlyersDf.count()) // top 100
        assertResult(358715)(flownTogetherDf.count()) // combination
        assertResult(15500)(longestRunDf.count())  //the same as number of passengers, one stat per passenger
    }
  }


}
