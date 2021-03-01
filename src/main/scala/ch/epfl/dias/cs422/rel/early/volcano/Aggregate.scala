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

  private var aggregated = Array.empty[Tuple]
  protected var aggregatedIterator: Iterator[Tuple] = Iterator()

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    val keyIndices = groupSet.toArray

    input.open()
    var next = input.next()
    if (next == NilTuple && groupSet.isEmpty) {
      val result: Tuple = aggCalls
        .map(aggEmptyValue)
        .foldLeft(IndexedSeq.empty[Elem])((a, b) => a :+ b)
      aggregated = Array(result)
    } else {
      var aggregates: Map[Tuple, Array[Tuple]] = Map.empty[Tuple, Array[Tuple]]
      while (next != NilTuple) {
        val tuple: Tuple = next.get
        val key: Tuple =
          keyIndices.map(i => tuple(i))
        aggregates = aggregates.get(key) match {
          case Some(arr: Array[Tuple]) => aggregates + (key -> arr.:+(tuple))
          case _                       => aggregates + (key -> Array(tuple))
        }
        next = input.next()
      }

      aggregated = aggCalls
        .map(agg => {
          val tuples: IndexedSeq[Tuple] = aggregates.toIndexedSeq.map {
            case (_, tuples) =>
              IndexedSeq(
                tuples
                  .map(tuple => agg.getArgument(tuple))
                  .reduce((a, b) => aggReduce(a, b, agg))
              )
          }
          tuples
        })
        .reduce((a, b) => a.zip(b).map { case (a, b) => a :++ b })
        .toArray
    }
    aggregatedIterator = aggregated.iterator
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] =
    if (aggregatedIterator.hasNext) {
      Some(aggregatedIterator.next())
    } else {
      NilTuple
    }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
  }
}
