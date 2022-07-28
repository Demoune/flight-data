package my.exercise.util

import org.apache.spark.sql.SparkSession

trait SparkSupport {

  lazy val spark: SparkSession = SparkSession.builder().getOrCreate()

}
