package my.exercise

import my.exercise.config.AppConfig.mainConfig
import my.exercise.util.LazyLog4j
import org.apache.spark.sql.DataFrame

class CsvDataWriter extends LazyLog4j {

  def write(df: DataFrame, datasetFolderName: String): Unit = {

    val baseOutputPath = if (mainConfig.output.basePath.endsWith("/")) mainConfig.output.basePath else mainConfig.output.basePath + "/"
    val folderLocation = baseOutputPath + datasetFolderName

    logger.info(s"Writing dataFrame to csv with header to this folder: ${folderLocation}")
    df.coalesce(1)
      .write
      .option("header", "true")
      .csv(folderLocation)
  }

}
