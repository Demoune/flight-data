package my.exercise

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, functions}

import java.sql.Date

class FlightDataCalc(val flightsDf: DataFrame, passengersDf: DataFrame) {

  /**
   * Finds the total number of flights for each month.
   *
   * @return DataFrame with result
   */
  def monthlyFlights(): DataFrame = {
    flightsDf
      .withColumn("Month", substring(col("date"), 0, 7))
      .groupBy("Month").count
      .orderBy("Month")
      .withColumnRenamed("count", "Number of Flights")
  }


  /**
   * Find the names of the 100 most frequent flyers.
   *
   * @param nLimit - how many top flyers to include into the result dataframe
   * @return DataFrame with result
   */
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

  /**
   * Finds the greatest number of countries a passenger has been in without being in the UK. For example,
   * if the countries a passenger was in were: UK -> FR -> US -> CN -> UK -> DE -> UK,
   * the correct answer would be 3 countries.
   *
   * @return DataFrame with result
   */
  def longestRunWithoutUK(): DataFrame = {

    val windowSpec = Window.partitionBy("passengerId").orderBy("date")

    val runGroups = flightsDf.withColumn("new_run", expr("from = 'uk'").cast("bigint"))
      .withColumn("run_num",
        functions.sum("new_run").over(windowSpec)) // breaking down passengers' flights by uk destination into groups

    //    runGroups.show(40, false)
    //    runGroups.where(col("passengerId").equalTo(lit("382"))).show(40, false)
    //    runGroups.where(col("passengerId").equalTo(lit("10111"))).show(40, false)

    val groupCounts = runGroups.groupBy("passengerId", "run_num")
      .agg(array_remove(array_union(collect_set("from"), collect_set("to")), "uk").as("destSet"))

    //    groupCounts.show(false)
    //    groupCounts.filter(col("passengerId").equalTo(lit("382"))).show(false)

    val groupCounts2 = groupCounts.withColumn("runLength", functions.size(col("destSet")))

    //    groupCounts.show(false)
    //    groupCounts2.where(col("passengerId").equalTo(lit("382"))).show(false)

    groupCounts2.groupBy("passengerId").agg(functions.max("runLength").as("Longest Run"))
      .withColumnRenamed("passengerId", "Passenger ID")
      .orderBy(desc("Longest Run"))
  }

  /**
   * Find the passengers who have been on more than N flights together.
   * You can specify optional date rang
   *
   * @param atLeastNTimes
   * @param from Optional from date
   * @param to   Optional to date
   * @return DataFrame with result
   */
  def flownTogether(atLeastNTimes: Int, from: Option[Date], to: Option[Date]): DataFrame = {

    var currDf = flightsDf

    if (from.nonEmpty && to.nonEmpty)
      currDf = flightsDf.withColumn("date2", col("date").cast("date"))
        .filter(col("date2").between(from.get, to.get))

    currDf.printSchema()

    currDf.filter(col("passengerId").equalTo("210")
      .or(col("passengerId").equalTo("271"))).show(false)

    currDf.as("a")
      .join(currDf.as("b"), //self-join
        col("a.flightId") === col("b.flightId") and
          col("a.passengerId") =!= col("b.passengerId") and
          col("a.passengerId") < col("b.passengerId") // using explicit join criteria to get rid of duplicates
      )
      .select(
        col("a.passengerId").as("Passenger1Id"),
        col("b.passengerId").as("Passenger2Id"),
        col("a.flightId")
      )
      .groupBy("Passenger1Id", "Passenger2Id")
      .agg(count(col("flightId")).as("Number of flights together"))
      .where(col("Number of flights together").gt(lit(atLeastNTimes - 1))) // filtering < 3 flights together
      .orderBy(desc("Number of flights together"))

  }

  def flowTogetherPreFilter(df: DataFrame, from: Date, to: Date): DataFrame = {
    df.withColumn("date2", col("date").cast("date"))
      .filter(col("date2").between(from, to)).drop("date2")
  }


}

object FlightDataCalc {
  def apply(flightsDf: DataFrame, passengersDf: DataFrame): FlightDataCalc =
    new FlightDataCalc(flightsDf: DataFrame, passengersDf: DataFrame)
}
