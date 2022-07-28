package my.exercise

import my.exercise.util.SparkSupport

/*
Main application class, to be specified for spark-submit
 */
object ExerciseMain extends App with SparkSupport {


  val flow: ExerciseFlow = new ExerciseFlow(spark)
  flow.run()

  spark.stop()

}

