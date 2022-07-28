package my.exercise

import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, DslSymbol, StringToAttributeConversionHelper}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{array_remove, array_union, broadcast, col, collect_list, collect_set, count, desc, expr, lag, last, lit, row_number, when}
import org.apache.spark.sql.sources.Not

import java.util.Date

class FlightData(val flightsDf: DataFrame, passengersDf: DataFrame) {

  def monthlyFlights(): DataFrame = {
    flightsDf.groupBy("_c4").count
  }

  def topNFlyers(nLimit: Int): DataFrame = {
    flightsDf.groupBy(col("passengerId"))
      .count()
      .orderBy(desc("count"))
      .limit(nLimit)
      .join(broadcast(passengersDf), usingColumns = Seq("passengerId"), joinType = "left") //we want to output flight stats even if passenger info is missing in the passenger file
      .select(
        col("passengerId").as("Passenger ID"),
        col("count").as("Number of Flights"),
        col("firstName").as("First Name"),
        col("lastName").as("Last Name")
      )
  }

  def longestRunWithoutUK(): DataFrame = {

    val windowSpec = Window.partitionBy("passengerId").orderBy("date")

    val runGroups = flightsDf.withColumn("new_run", expr("from = 'uk'").cast("bigint"))
      .withColumn("run_num",
        functions.sum("new_run").over(windowSpec))

    runGroups.show(40, false)
    runGroups.where(col("passengerId").equalTo(lit("382"))).show(40, false)

    //    val groupCounts = runGroups
    //        .groupBy("passengerId", "run_num").agg(count("*").as("Run Length"))
//    val groupCounts = runGroups.groupBy("passengerId", "run_num")
//      .agg(functions.size(array_remove(collect_set("from"), "uk")).as("runLength"))

    val groupCounts = runGroups.groupBy("passengerId", "run_num")
      .agg(array_remove(array_union(collect_set("from"), collect_set("to")) , "uk").as("temp1"))

//    groupCounts.show(false)
//    groupCounts.where(col("passengerId").equalTo(lit("382"))).show(false)

    val groupCounts2 = groupCounts.withColumn("runLength", functions.size(col("temp1")))

    //debug
//    groupCounts.show(false)
//    groupCounts2.where(col("passengerId").equalTo(lit("382"))).show(false)

    groupCounts2.groupBy("passengerId").agg(functions.max("runLength").as("Longest Run"))
          .withColumnRenamed("passengerId", "Passenger ID")
  }

  def flownTogether(atLeastNTimes: Int, from: Date, to: Date): DataFrame = {
    ???
  }

}

object FlightData {
  def apply(flightsDf: DataFrame, passengersDf: DataFrame): FlightData =
    new FlightData(flightsDf: DataFrame, passengersDf: DataFrame)
}
