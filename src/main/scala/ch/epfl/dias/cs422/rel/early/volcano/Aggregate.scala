package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, NilTuple, Tuple}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  * @see [[ch.epfl.dias.cs422.helpers.rex.AggregateCall]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  protected var lazyAggregated: LazyList[Tuple] = LazyList.empty[Tuple]

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()
    var next = input.next()
    if (next == NilTuple && groupSet.isEmpty) {
      // return aggEmptyValue for each aggregate.
      val result: Tuple = aggCalls
        .map(aggEmptyValue)
        .foldLeft(IndexedSeq.empty[Elem])((a, b) => a :+ b)
      lazyAggregated = result #:: LazyList.empty[Tuple]
    } else {

      // Group based on the key produced by the indices in groupSet
      val keyIndices = groupSet.toArray
      var aggregates: Map[Tuple, Array[Tuple]] = Map.empty[Tuple, Array[Tuple]]
      while (next != NilTuple) {
        val tuple: Tuple = next.get
        val key: Tuple = keyIndices.map(i => tuple(i))
        aggregates = aggregates.get(key) match {
          case Some(arr: Array[Tuple]) => aggregates + (key -> arr.:+(tuple))
          case _                       => aggregates + (key -> Array(tuple))
        }
        next = input.next()
      }

      def aggregate(tuples: List[(Tuple, Array[Tuple])]): LazyList[Tuple] =
        tuples match {
          case (key, tuples) :: tail =>
            key.++(
              aggCalls.map(agg =>
                tuples.map(t => agg.getArgument(t)).reduce(aggReduce(_, _, agg))
              )
            ) #:: aggregate(tail)
          case _ => LazyList.empty[Tuple]
        }
      lazyAggregated = aggregate(aggregates.toList)
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] =
    lazyAggregated match {
      case LazyList() => NilTuple
      case tuple #:: tail =>
        lazyAggregated = tail
        Some(tuple)
    }

  /**
    * @inheritdoc
    */
  override def close(): Unit = input.close()
}
