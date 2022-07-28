package my.exercise

import org.apache.spark.sql.{DataFrame, SparkSession}

class CsvDataReader(implicit spark: SparkSession) {

  def read(location: String): DataFrame = {
    spark.read.option("header", true).csv(location)
  }
}
