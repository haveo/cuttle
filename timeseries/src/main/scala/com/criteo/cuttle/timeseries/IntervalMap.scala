package com.criteo.cuttle.timeseries

import scala.math.Ordered.{orderingToOrdered}

import de.sciss.fingertree.{FingerTree, Measure}

import FingerTree._

object IntervalMap {

  private class MeasureKey[A, B] extends Measure[(A, B), Option[A]] {
    override def toString = "Key"

    val zero: Option[A] = None
    def apply(x: (A, B)) = Some(x._1)

    def |+|(a: Option[A], b: Option[A]) = b
  }
  private implicit def measure[A, B] = new MeasureKey[Interval[A], B]

  case class Interval[V](lo: V, hi: V)
  private type Elem[A, B] = (Interval[A], B)

  def apply[A: Ordering, B](elems: Elem[A, B]*): IntervalMap[A, B] = {
    val sortedElems = elems.sortBy(_._1.lo)
    val isOverlapping = sortedElems.sliding(2).exists {
      case List((Interval(_, hi), v1), (Interval(lo, _), v2)) =>
        hi > lo || (hi == lo && v1 == v2)
      case _ => false
    }
    if (isOverlapping)
      throw new IllegalArgumentException("non-canonical intervals")

    new Impl(FingerTree(sortedElems: _*))
  }

  def empty[A: Ordering, B]: IntervalMap[A, B] =
    new Impl(FingerTree.empty)

  private class Impl[A: Ordering, B](protected val tree: FingerTree[Option[Interval[A]], Elem[A, B]])
      extends IntervalMap[A, B] {
    def toList = tree.toList

    def lower(interval: Interval[A]): (Option[Interval[A]] => Boolean) = {
      case None => true
      case Some(Interval(lo, _)) => lo < interval.lo
    }

    def greater(interval: Interval[A]): Option[Interval[A]] => Boolean = {
      case None => true
      case Some(Interval(_, hi)) => hi <= interval.hi
    }

    def intersect(interval: Interval[A]) = {
      val (leftOfLow, rightOfLow) = tree.span(lower(interval))

      val withoutLow = leftOfLow.viewRight match {
        case ViewRightCons(_, (Interval(lo, hi), v)) if hi > interval.lo =>
          (Interval(interval.lo, hi) -> v) +: rightOfLow
        case _ =>
          rightOfLow
      }
      val (leftOfHigh, rightOfHigh) = withoutLow.span(greater(interval))

      val newTree = rightOfHigh.viewLeft match {
        case ViewLeftCons((Interval(lo, hi), v), _) if lo < interval.hi =>
          leftOfHigh :+ (Interval(lo, interval.hi) -> v)
        case ViewNil() =>
          leftOfHigh
      }

      new Impl(newTree)
    }

    def update(interval: Interval[A], value: B) = {
      val left = tree.takeWhile(lower(interval))
      val (remainingLeft, newLo) = left.viewRight match {
        case ViewRightCons(init, (Interval(lo, hi), v)) if hi >= interval.lo =>
          if (v == value)
            (init, lo)
          else
            (init :+ (Interval(lo, interval.lo) -> v), interval.lo)
        case _ =>
          (left, interval.lo)
      }

      val right = tree.dropWhile(greater(interval))
      val (newHi, remainingRight) = right.viewLeft match {
        case ViewLeftCons((Interval(lo, hi), v), tail) if lo <= interval.hi =>
          if (v == value)
            (hi, tail)
          else
            (interval.hi, (Interval(interval.hi, hi) -> v) +: tail)
        case _ =>
          (interval.hi, right)
      }

      new Impl((remainingLeft :+ (Interval(newLo, newHi) -> value)) ++ remainingRight)
    }
  }

}

sealed trait IntervalMap[A, B] {
  import IntervalMap._

  def toList: List[Elem[A, B]]
  def update(interval: Interval[A], value: B): IntervalMap[A, B]
  def intersect(interval: Interval[A]): IntervalMap[A, B]
}
