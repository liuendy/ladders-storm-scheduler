package com.theladders.storm

import scala.annotation.tailrec

trait Interleaver {
  /*
   * Given
   *   List( List(1,4,7), List(2,5,8), List(3,6,9) )
   * Return
   *   List( 1, 2, 3, 4, 5, 6, 7, 8, 9 )
   *
   * This works with input with variable size internal lists as well.
   *
   * Given
   *   List( List(1,4), List(2), List(3,6) )
   * Return
   *   List( 1, 2, 3, 4, 6 )
   */
  def interleaveFlattened[T](values: List[List[T]]): List[T] = {
    interleaveFlattened(values, Nil)
  }

  @tailrec
  private def interleaveFlattened[T](items: List[List[T]],
                                     accumulatedResult: List[T]): List[T] = {
    if (items.isEmpty) return accumulatedResult

    val firstsAndRests = items.map(split)
    val firsts = firstsAndRests.flatMap(_._1)
    val rests = firstsAndRests.map(_._2)

    val left = rests.map(_.size).sum
    if (left == 0) {
      accumulatedResult ++ firsts
    } else {
      interleaveFlattened(rests, accumulatedResult ++ firsts)
    }
  }

  /*
   * Given
   *   List( List(1,4,7), List(2,5,8), List(3,6,9) )
   * Return
   *   List( List(1, 2, 3), List(4, 5, 6), List(7, 8, 9) )
   *
   * This works with input with variable size internal lists as well.
   *
   * Given
   *   List( List(1,4), List(2), List(3,6) )
   * Return
   *   List( List(1, 2, 3), List(4, 6) )
   */
  def interleave[T](values: List[List[T]]): List[List[T]] = {
    interleave(values, Nil)
  }

  @tailrec
  private def interleave[T](items: List[List[T]],
                            accumulatedResult: List[List[T]]): List[List[T]] = {
    if (items.isEmpty) return accumulatedResult

    val firstsAndRests = items.map(split)
    val firsts = firstsAndRests.flatMap(_._1)
    val rests = firstsAndRests.map(_._2)

    val combined = accumulatedResult.+:(firsts)

    val left = rests.map(_.size).sum
    if (left == 0) {
      combined
    } else {
      interleave(rests, combined)
    }
  }

  /* Split list into first element and the rest */
  private def split[T](list: List[T]): (Option[T], List[T]) = list match {
    case Nil => (None, List.empty)
    case last :: Nil => (Some(last), List.empty)
    case first :: rest => (Some(first), rest)
  }
}
