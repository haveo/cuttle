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
  import TimeSeriesGrid._

  private implicit val intervalEncoder = new Encoder[Interval[Instant]] {
    implicit val boundEncoder = new Encoder[Bound[Instant]] {
      override def apply(bound: Bound[Instant]) = bound match {
        case Bottom => "-oo".asJson
        case Top => "+oo".asJson
        case Finite(t) => t.asJson
      }
    }
    override def apply(interval: Interval[Instant]) =
      Json.obj(
        "start" -> interval.lo.asJson,
        "end" -> interval.hi.asJson
      )
  }

  private[cuttle] override def routes(workflow: Workflow[TimeSeries],
                                      executor: Executor[TimeSeries],
                                      xa: XA): PartialService = {

    case request @ GET at url"/api/timeseries/calendar?jobs=$jobs" =>

      val filteredJobs = Try(jobs.split(",").toSeq.filter(_.nonEmpty)).toOption
        .filter(_.nonEmpty)
        .getOrElse(workflow.vertices.map(_.id))
        .toSet
      def watchState() = Some((state, executor.allFailing))
      def getCalendar(watchedValue: Any = ()) = {
        val (state, backfills) = this.state
        val upToNow = Interval(Bottom, Finite(Instant.now))
        (for {
          job <- workflow.vertices
          if filteredJobs.contains(job.id)
          (interval, jobState) <- state(job).intersect(upToNow).toList
          (start, end) <- Daily(UTC).split(interval)
        } yield
          (Daily(UTC).truncate(start), start.until(end, SECONDS), jobState == Done, jobState match {
            case Running(exec) => executor.allFailingExecutions.contains(exec)
            case _ => false
          }))
          .groupBy(_._1)
          .toList
          .map {
            case (date, set) =>
              val (total, done, stuck) = set.foldLeft((0L, 0L, false)) { (acc, exec) =>
                val (totalDuration, doneDuration, isAnyStuck) = acc
                val (_, duration, isDone, isStuck) = exec
                val newDone = if (isDone) duration else 0L
                (totalDuration + duration, doneDuration + newDone, isAnyStuck || isStuck)
              }
              Map(
                "date" -> date.asJson,
                "completion" -> f"${done.toDouble / total.toDouble}%1.1f".asJson
              ) ++ (if (stuck) Map("stuck" -> true.asJson) else Map.empty) ++
              // unfiltered backfills
                (if (backfills.intersect(Interval(date, Daily(UTC).next(date))).toList.nonEmpty)
                   Map("backfill" -> true.asJson)
                 else Map.empty)
          }
          .asJson

      }
      if (request.headers.get(h"Accept").exists(_ == h"text/event-stream"))
        sse(watchState, getCalendar)
      else
        Ok(getCalendar())

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
