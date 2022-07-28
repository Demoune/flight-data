package my.exercise.util

import org.apache.log4j.Logger

trait LazyLog4j {

  @transient
  protected lazy val logger = Logger.getLogger(this.getClass)

}
