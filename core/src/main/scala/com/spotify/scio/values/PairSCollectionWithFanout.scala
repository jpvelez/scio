package com.spotify.scio.values

import com.google.cloud.dataflow.sdk.transforms.Combine.PerKeyWithHotKeyFanout
import com.google.cloud.dataflow.sdk.transforms.{Combine, SerializableFunction}
import com.spotify.scio.util.Functions
import com.spotify.scio.util.TupleFunctions._
import com.twitter.algebird.{Semigroup, Monoid}

import scala.reflect.ClassTag

/**
 * An enhanced SCollection that uses an intermediate node to combine "hot" keys partially before performing the full combine.
 */
class PairSCollectionWithFanout[K: ClassTag, V: ClassTag](val self: PairSCollectionFunctions[K, V],
                                                          private val fanout: Either[K => Int, Int]) {

  private def withFanout[K, I, O](combine: Combine.PerKey[K, I, O]): PerKeyWithHotKeyFanout[K, I, O] = this.fanout match {
    case Left(f) =>
      combine.withHotKeyFanout(Functions.serializableFn(f).asInstanceOf[SerializableFunction[K, java.lang.Integer]])
    case Right(f) =>
      combine.withHotKeyFanout(f)
  }

  /**
   * Aggregate the values of each key, using given combine functions and a neutral "zero value".
   * This function can return a different result type, U, than the type of the values in this
   * SCollection, V. Thus, we need one operation for merging a V into a U and one operation for
   * merging two U's. To avoid memory allocation, both of these functions are allowed to modify
   * and return their first argument instead of creating a new U.
   */
  def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U, combOp: (U, U) => U): SCollection[(K, U)] =
    self.applyPerKey(withFanout(Combine.perKey(Functions.aggregateFn(zeroValue)(seqOp, combOp))), kvToTuple[K, U])

  /**
   * Generic function to combine the elements for each key using a custom set of aggregation
   * functions. Turns an SCollection[(K, V)] into a result of type SCollection[(K, C)], for a
   * "combined type" C Note that V and C can be different -- for example, one might group an
   * SCollection of type (Int, Int) into an RDD of type (Int, Seq[Int]). Users provide three
   * functions:
   *
   * - `createCombiner`, which turns a V into a C (e.g., creates a one-element list)
   *
   * - `mergeValue`, to merge a V into a C (e.g., adds it to the end of a list)
   *
   * - `mergeCombiners`, to combine two C's into a single one.
   */
  def combineByKey[C: ClassTag](createCombiner: V => C)
                               (mergeValue: (C, V) => C)
                               (mergeCombiners: (C, C) => C): SCollection[(K, C)] =
    self.applyPerKey(withFanout(Combine.perKey(Functions.combineFn(createCombiner, mergeValue, mergeCombiners))), kvToTuple[K, C])

  /**
   * Merge the values for each key using an associative function and a neutral "zero value" which
   * may be added to the result an arbitrary number of times, and must not change the result
   * (e.g., Nil for list concatenation, 0 for addition, or 1 for multiplication.).
   * @group per_key
   */
  def foldByKey(zeroValue: V)(op: (V, V) => V): SCollection[(K, V)] =
    self.applyPerKey(withFanout(Combine.perKey(Functions.aggregateFn(zeroValue)(op, op))), kvToTuple[K, V])

  /**
   * Fold by key with [[com.twitter.algebird.Monoid Monoid]], which defines the associative
   * function and "zero value" for V. This could be more powerful and better optimized in some
   * cases.
   * @group per_key
   */
  def foldByKey(implicit mon: Monoid[V]): SCollection[(K, V)] =
    self.applyPerKey(withFanout(Combine.perKey(Functions.reduceFn(mon))), kvToTuple[K, V])

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce.
   */
  def reduceByKey(op: (V, V) => V): SCollection[(K, V)] =
    self.applyPerKey(withFanout(Combine.perKey(Functions.reduceFn(op))), kvToTuple[K, V])

  /**
   * Reduce by key with [[com.twitter.algebird.Semigroup Semigroup]]. This could be more powerful
   * and better optimized in some cases.
   */
  def sumByKey(implicit sg: Semigroup[V]): SCollection[(K, V)] =
    self.applyPerKey(withFanout(Combine.perKey(Functions.reduceFn(sg))), kvToTuple[K, V])

}