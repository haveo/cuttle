package com.criteo.cuttle.timeseries

import TimeSeriesUtils._

import com.criteo.cuttle._

import lol.http._
import lol.json._

import io.circe._
import io.circe.syntax._

import algebra.lattice.Bool._

import scala.util.{Try}
import scala.math.Ordering.Implicits._

import continuum._
import continuum.bound._

import java.time.{Instant, ZoneId}
import java.time.temporal.ChronoUnit._
import java.util.UUID

import ExecutionStatus._

private[timeseries] trait TimeSeriesApp { self: TimeSeriesScheduler =>
  import App._

  private implicit val intervalEncoder = new Encoder[Interval[Instant]] {
    override def apply(interval: Interval[Instant]) = {
      val Closed(start) = interval.lower.bound
      val Open(end) = interval.upper.bound
      Json.obj(
        "start" -> start.asJson,
        "end" -> end.asJson
      )
    }
  }

  private[cuttle] override def routes(workflow: Workflow[TimeSeries],
                                      executor: Executor[TimeSeries],
                                      xa: XA): PartialService = {

    case POST at url"/api/timeseries/backfill?jobs=$jobsString&startDate=$start&endDate=$end&priority=$priority" =>
      val jobIds = jobsString.split(",")
      //val jobs = this.state._1.keySet.filter((job: TimeSeriesJob) => jobIds.contains(job.id))
      val startDate = Instant.parse(start)
      val endDate = Instant.parse(end)
      val backfillId = UUID.randomUUID().toString
      backfillJob(backfillId, Set(), startDate, endDate, priority.toInt)
      Ok(
        Json.obj(
          "id" -> backfillId.asJson
        ))
  }
}
