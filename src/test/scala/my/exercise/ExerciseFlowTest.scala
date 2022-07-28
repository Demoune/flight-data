package my.exercise

import my.exercise.config.AppConfig.mainConfig
import my.exercise.utils.TestUtils
import org.scalatest.BeforeAndAfter
import org.scalatest.funspec.AnyFunSpec

import java.io.File
import java.nio.file.{Path, Paths}

class ExerciseFlowTest extends AnyFunSpec with SparkTestSupport with BeforeAndAfter {

  before {
    val rootFolder = new File(mainConfig.output.basePath)
    if (rootFolder.exists()) {
      TestUtils.remove(Paths.get(mainConfig.output.basePath), true)
    }
  }

  describe("ExerciseFlow run") {
    it("should successfully read 2 input files and generate 4 csv files in output locations") {
      val exerciseFlow = new ExerciseFlow(spark)
      exerciseFlow.run()

      val csvReader = new CsvDataReader(spark)
      val flownTogetherDf = csvReader.read(mainConfig.output.basePath + "/" + "flown-together-min3/*.csv" )
      val longestRunDf = csvReader.read(mainConfig.output.basePath + "/" + "longest-run-without-uk/*.csv" )
      val monthlyFlightsDf = csvReader.read(mainConfig.output.basePath + "/" + "monthly-flights/*.csv" )
      val top100FlyersDf = csvReader.read(mainConfig.output.basePath + "/" + "top-100-flyers/*.csv" )

      it("monthlyFlightsDf should contain 12 records") {
        assertResult(12)(monthlyFlightsDf.count())
      }
      it("top100FlyersDf should contain 100 records") {
        assertResult(100)(top100FlyersDf.count())
      }
      it("flownTogetherDf should contain 222 records") {
        assertResult(222)(flownTogetherDf.count())
      }
      it("longestRunDf should contain 444 records") {
        assertResult(444)(longestRunDf.count())
      }
    }
  }


}
