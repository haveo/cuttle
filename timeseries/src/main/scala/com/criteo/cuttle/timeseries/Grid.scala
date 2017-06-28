package com.criteo.cuttle.timeseries

trait Grid[A] {
  def toInterval(slot: A): Interval[Instant]
  def next(slot: A): A
}

case class Hourly(n: Int)
case class Daily(n: Int)

object Grid {
  def apply[A: Grid]: Grid[A] = implicitly

  implicit object HourlyGrid extends Grid[Hourly] {
    def toInt(value: Hourly) = value.n
  }

  implicit object DailyGrid extends Grid[Daily] {
    def toInt(value: Daily) = value.n
  }

  implicit class GridOps[A: Grid](thing: A) {
    def toInt = Grid[A].toInt(thing)
  }

}
