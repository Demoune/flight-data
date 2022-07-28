package my.exercise.config

import pureconfig.ConfigSource
import pureconfig.generic.auto.exportReader

final case class Input(flightDataFile: String, passengerDataFile: String)

final case class Output(basePath: String)

case class ExerciseConfig(input: Input, output: Output)

object AppConfig {

  val mainConfig: ExerciseConfig = ConfigSource.default.loadOrThrow[ExerciseConfig]

}
