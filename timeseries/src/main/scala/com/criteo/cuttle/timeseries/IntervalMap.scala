package com.criteo.cuttle.timeseries

import scala.math.Ordered.{orderingToOrdered}

import cats._
import cats.data._
import cats.implicits._


import de.sciss.fingertree.{FingerTree, Measure}

import FingerTree._

sealed trait Bound[+V]
case object Bottom extends Bound[Nothing]
case object Top extends Bound[Nothing]
case class Finite[V](bound: V) extends Bound[V]
object Bound {
  implicit def ordering[V: Ordering]: Ordering[Bound[V]] =
    Ordering.by[Bound[V], (Int, Option[V])] {
      case Bottom => (-1, None)
      case Top => (1, None)
      case Finite(v) => (0, Some(v))
    }
    implicit def functorInstance: Functor[Bound] = new Functor[Bound] {
      def map[A, B](bound: Bound[A])(f: A => B): Bound[B] = bound match {
        case Finite(v) => Finite(f(v))
        case Bottom => Bottom
        case Top => Top
      }
    }
}

case class Interval[V: Ordering](lo: Bound[V], hi: Bound[V]) {
  if (lo >= hi)
    throw new IllegalArgumentException("low bound must be smaller than high bound")
  def map[A: Ordering](f: V => A): Interval[A] =
    Interval(lo.map(f), hi.map(f))
}
object Interval {
  def apply[V: Ordering](lo: V, hi: V): Interval[V] =
    Interval(Finite(lo), Finite(hi))
}

object IntervalMap {

  private class MeasureKey[A, B] extends Measure[(A, B), Option[A]] {
    override def toString = "Key"

    val zero: Option[A] = None
    def apply(x: (A, B)) = Some(x._1)

    def |+|(a: Option[A], b: Option[A]) = b
  }
  private implicit def measure[A, B] = new MeasureKey[Interval[A], B]

  sealed trait Bound[+V]
  case object Bottom extends Bound[Nothing]
  case object Top extends Bound[Nothing]
  case class Left[V](bound: V) extends Bound[V]
  case class Right[V](bound: V) extends Bound[V]
  object Bound {
    implicit def ordering[V: Ordering]: Ordering[Bound[V]] =
      Ordering.by[Bound[V], (Int, Option[(V, Int)])] {
        case Bottom => (-1, None)
        case Top => (1, None)
        case Left(v) => (0, Some((v, -1)))
        case Right(v) => (0, Some((v, 1)))
      }
  }

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

  private class Impl[A: Ordering, B](val tree: FingerTree[Option[Interval[A]], Elem[A, B]])
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

    def mapKeys[K: Ordering](f: A => K) = new Impl(FingerTree(tree.toList.map {
      case (interval, v) => (interval.map(f) -> v)
    }: _*))

    def whenIsDef[C](other: IntervalMap[A, C]) = this
  }

  implicit def functorFilterInstance[K: Ordering] =
    new FunctorFilter[({type λ[α] = IntervalMap[K, α]})#λ] {
      def map[A, B](m: IntervalMap[K, A])(f: A => B) = m match {
        case impl: Impl[K, A] =>
          new Impl(FingerTree[Option[Interval[K]], Elem[K, B]](impl.tree.toList.map {
            case (itvl, v) => itvl -> f(v)
          } : _*))
      }
      def mapFilter[A, B](m: IntervalMap[K, A])(f: A => Option[B]) = m match {
        case impl: Impl[K, A] =>
          new Impl(FingerTree[Option[Interval[K]], Elem[K, B]](impl.tree.toList.mapFilter {
            case (itvl, v) => f(v).map(itvl -> _)
          } : _*))
      }
    }

}

sealed trait IntervalMap[A, B] {
  import IntervalMap._

  def toList: List[Elem[A, B]]
  def update(interval: Interval[A], value: B): IntervalMap[A, B]
  def intersect(interval: Interval[A]): IntervalMap[A, B]
  def mapKeys[K: Ordering](f: A => K): IntervalMap[K, B]
  def whenIsDef[C](other: IntervalMap[A, C]): IntervalMap[A, B]
  def whenIsUndef[C](other: IntervalMap[A, C]) = this
}
