package my.exercise.utils

import org.apache.spark.sql.SparkSession

trait SparkTestSupport {

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("Flight-data local test")
    .getOrCreate()

}
