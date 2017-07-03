package com.criteo.cuttle.timeseries

import Internal._
import com.criteo.cuttle._

import scala.concurrent._
import scala.concurrent.duration.{Duration => ScalaDuration}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.stm._

import cats.implicits._

import io.circe._
import io.circe.syntax._
import io.circe.generic.semiauto._

import doobie.imports._

import java.time._
import java.time.temporal.ChronoUnit._
import java.time.temporal._

sealed trait TimeSeriesGrid {
  def next(t: Instant): Instant
  def truncate(t: Instant): Instant
  def ceil(t: Instant): Instant = {
    val truncated = truncate(t)
    if (truncated == t) t
    else next(t)
  }
  def inInterval(interval: Interval[Instant], maxPeriods: Int) = {
    def go(lo: Instant, hi: Instant): List[(Instant, Instant)] = {
      val nextLo = next(lo)
      if (nextLo.isAfter(hi)) List.empty
      else ((lo, nextLo) +: go(nextLo, hi))
    }
    interval match {
      case Interval(Finite(lo), Finite(hi)) =>
        go(ceil(lo), hi).grouped(maxPeriods).map(xs => (xs.head._1, xs.last._2))
    }
  }
  def split(interval: Interval[Instant]) = {
    def go(lo: Instant, hi: Instant): List[(Instant, Instant)] = {
      val nextLo = next(lo)
      if (nextLo.isBefore(hi)) ((lo, nextLo) +: go(nextLo, hi))
      else List((lo, hi))
    }
    interval match {
      case Interval(Finite(lo), Finite(hi)) =>
        go(lo, hi)
    }
  }
}
object TimeSeriesGrid {
  case object Hourly extends TimeSeriesGrid {
    def truncate(t: Instant) = t.truncatedTo(HOURS)
    def next(t: Instant) =
      t.truncatedTo(HOURS).plus(1, HOURS)
  }
  case class Daily(tz: ZoneId) extends TimeSeriesGrid {
    def truncate(t: Instant) = t.atZone(tz).truncatedTo(DAYS).toInstant
    def next(t: Instant) = t.atZone(tz).truncatedTo(DAYS).plus(1, DAYS).toInstant
  }
  private[timeseries] case object Continuous extends TimeSeriesGrid {
    def next(t: Instant) = ???
    def truncate(t: Instant) = ???
  }

  implicit val gridEncoder = new Encoder[TimeSeriesGrid] {
    override def apply(grid: TimeSeriesGrid) = grid match {
      case Hourly => Json.obj("period" -> "hourly".asJson)
      case Daily(tz: ZoneId) =>
        Json.obj(
          "period" -> "daily".asJson,
          "zoneId" -> tz.getId().asJson
        )
      case Continuous => Json.obj("period" -> "continuuous".asJson)
    }
  }
}

import TimeSeriesGrid._

case class Backfill(id: String, start: Instant, end: Instant, jobs: Set[Job[TimeSeries]], priority: Int)
private[timeseries] object Backfill {
  implicit val encoder: Encoder[Backfill] = deriveEncoder
  implicit def decoder(implicit jobs: Set[Job[TimeSeries]]) =
    deriveDecoder[Backfill]
}

case class TimeSeriesContext(start: Instant, end: Instant, backfill: Option[Backfill] = None)
    extends SchedulingContext {

  def toJson: Json = this.asJson

  def log: ConnectionIO[String] = Database.serializeContext(this)

  def toInterval: Interval[Instant] = Interval(start, end)

  def compareTo(other: SchedulingContext) = other match {
    case TimeSeriesContext(otherStart, _, otherBackfil) =>
      val priority: (Option[Backfill] => Int) = _.map(_.priority).getOrElse(0)
      val thisBackfillPriority = priority(backfill)
      val otherBackfillPriority = priority(otherBackfil)
      if (thisBackfillPriority == otherBackfillPriority) {
        start.compareTo(otherStart)
      } else {
        thisBackfillPriority.compareTo(otherBackfillPriority)
      }
  }
}

object TimeSeriesContext {
  private[timeseries] implicit val encoder: Encoder[TimeSeriesContext] = deriveEncoder
  private[timeseries] implicit def decoder(implicit jobs: Set[Job[TimeSeries]]): Decoder[TimeSeriesContext] =
    deriveDecoder
}

case class TimeSeriesDependency(offset: Duration)

case class TimeSeries(grid: TimeSeriesGrid, start: Instant, maxPeriods: Int = 1) extends Scheduling {
  import TimeSeriesGrid._
  type Context = TimeSeriesContext
  type DependencyDescriptor = TimeSeriesDependency
  def toJson: Json =
    Json.obj(
      "start" -> start.asJson,
      "maxPeriods" -> maxPeriods.asJson,
      "grid" -> grid.asJson
    )
}

object TimeSeries {
  implicit def scheduler = TimeSeriesScheduler()
}

sealed trait JobState
case object Done extends JobState
case class Todo(maybeBackfill: Option[Backfill]) extends JobState
case class Running(executionId: String) extends JobState

case class TimeSeriesScheduler() extends Scheduler[TimeSeries] with TimeSeriesApp {
  import TimeSeriesUtils._

  val allContexts = Database.sqlGetContextsBetween(None, None)

  private val timer =
    Job("timer", TimeSeries(Continuous, Instant.ofEpochMilli(0)))(_ => sys.error("panic!"))

  private val _state = Ref(Map.empty[TimeSeriesJob, IntervalMap[Instant, JobState]])

  private val _backfills = TSet.empty[Backfill]

  private[timeseries] def state: (State, IntervalMap[Instant, Unit]) = atomic { implicit txn =>
    val backfillDomain =
      _backfills.snapshot.foldLeft(IntervalMap.empty[Instant, Unit]) { (acc, bf) =>
        acc.update(Interval(bf.start, bf.end), ())
      }
    (_state(), backfillDomain)
  }

  private[timeseries] def backfillJob(id: String,
                                      jobs: Set[TimeSeriesJob],
                                      start: Instant,
                                      end: Instant,
                                      priority: Int) = Left("unimplemented")
//    atomic { implicit txn =>
//      val newBackfill = Backfill(id, start, end, jobs, priority)
//      val newBackfillDomain = backfillDomain(newBackfill)
//      if (jobs.exists(job => newBackfill.start.isBefore(job.scheduling.start))) {
//        Left("cannot backfill before a job's start date")
//      } else if (_backfills.exists(backfill => and(backfillDomain(backfill), newBackfillDomain) != zero[StateD])) {
//        Left("intersects with another backfill")
//      } else if (newBackfillDomain.defined.exists {
//                   case (job, is) =>
//                     is.exists { interval =>
//                       IntervalSet(interval) -- IntervalSet(
//                         splitInterval(job, interval, true).map(_.toInterval).toSeq: _*) != IntervalSet.empty[Instant]
//                     }
//                 }) {
//        Left("cannot backfill partial periods")
//      } else {
//        _backfills += newBackfill
//        _state() = _state() ++ jobs.map((job: TimeSeriesJob) =>
//          job -> (_state().apply(job) - Interval.closedOpen(start, end)))
//        Right(id)
//      }
//    }

  def start(workflow: Workflow[TimeSeries], executor: Executor[TimeSeries], xa: XA): Unit = {
    Database.doSchemaUpdates.transact(xa).unsafePerformIO

//    Database
//      .deserialize(workflow.vertices)
//      .transact(xa)
//      .unsafePerformIO
//      .foreach {
//        case (state, backfillState) =>
//          atomic { implicit txn =>
//            _state() = _state() ++ state
//            _backfills ++= backfillState
//          }
//      }

    atomic { implicit txn =>
      workflow.vertices.foreach { job =>
        if (!_state().contains(job)) {
          _state() = _state() + (job -> IntervalMap(Interval(Finite(job.scheduling.start), Top) -> Todo(None)))
        }
      }
    }

    def go(running: Set[Run]): Unit = {
      val (completed, stillRunning) = running.partition(_._3.isCompleted)
      val now = Instant.now
      val (stateSnapshot, backfillSnapshot, toRun) = atomic { implicit txn =>
        completed.foreach {
          case (job, context, future) =>
            val jobState = if (future.value.get.isSuccess) Done else Todo(context.backfill)
            _state() = _state() + (job ->
              (_state().apply(job).update(context.toInterval, jobState)))
        }

        val _toRun = next(
          workflow,
          _state(),
          now
        )

        (_state(), _backfills.snapshot, _toRun)
      }

      val newExecutions = executor.runAll(toRun)

      atomic { implicit txn =>
        newExecutions.foldLeft(_state()) { (st, x) =>
          val (execution, result) = x
          st + (execution.job ->
            st(execution.job).update(execution.context.toInterval, Running(execution.id)))
        }
      }

      // if (completed.nonEmpty || toRun.nonEmpty)
      //   Database.serialize(stateSnapshot, backfillSnapshot).transact(xa).unsafePerformIO

      val newRunning = stillRunning ++ newExecutions.map {
        case (execution, result) =>
          (execution.job, execution.context, result)
      }

      Future.firstCompletedOf(utils.Timeout(ScalaDuration.create(1, "s")) :: newRunning.map(_._3).toList).andThen {
        case _ => go(newRunning ++ stillRunning)
      }
    }

    go(Set.empty)
  }

  private[timeseries] def next(workflow0: Workflow[TimeSeries], state0: State, now: Instant): List[Executable] = {
    val workflow = workflow0 dependsOn timer
    val timerInterval = IntervalMap(Interval(Bottom, Finite(now)) -> (Done: JobState))
    val state = state0 + (timer -> timerInterval)

    val parentsMap = workflow.edges.groupBy { case (child, parent, _) => child }
    val childrenMap = workflow.edges.groupBy { case (child, parent, _) => parent }

    (for {
      job <- workflow0.vertices.toList
    } yield {
      val dependenciesSatisfied = parentsMap(job)
        .map {
          case (_, parent, lbl) =>
            state(parent).mapKeys(_.plus(lbl.offset)).collect { case Done => () }
        }
        .reduce(_ whenIsDef _)
      val noChildrenRunning = childrenMap.getOrElse(job, Set.empty)
        .map {
          case (child, _, lbl) =>
            state(child).mapKeys(_.minus(lbl.offset)).collect { case Running(_) => () }
        }
      .fold(IntervalMap(Interval.full[Instant] -> {()}))(_ whenIsUndef _)
      val toRun = state(job)
        .collect { case Todo(maybeBackfill) => maybeBackfill }
        .whenIsDef(dependenciesSatisfied)
        .whenIsDef(noChildrenRunning)

      for {
        (interval, maybeBackfill) <- toRun.toList
        (lo, hi) <- job.scheduling.grid.inInterval(interval, job.scheduling.maxPeriods)
      } yield {
        (job, TimeSeriesContext(lo, hi, maybeBackfill))
      }
    }).flatten

  }
}

private[timeseries] object TimeSeriesUtils {
  type TimeSeriesJob = Job[TimeSeries]
  type Executable = (TimeSeriesJob, TimeSeriesContext)
  type Run = (TimeSeriesJob, TimeSeriesContext, Future[Unit])
  type State = Map[TimeSeriesJob, IntervalMap[Instant, JobState]]

  val UTC = ZoneId.of("UTC")
}
