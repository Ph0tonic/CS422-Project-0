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

  protected var aggregated = List.empty[(Tuple, Vector[Tuple])]

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()
    var next = input.next()
    if (next == NilTuple && groupSet.isEmpty) {
      // return aggEmptyValue for each aggregate.
      aggregated = List(
        (IndexedSeq.empty[Elem] -> Vector(
          aggCalls
            .map(aggEmptyValue)
            .foldLeft(IndexedSeq.empty[Elem])((a, b) => a :+ b)
            .asInstanceOf[Tuple]
        ))
      )

    } else {
      // Group based on the key produced by the indices in groupSet
      val keyIndices = groupSet.toArray
      var aggregates = Map.empty[Tuple, Vector[Tuple]]
      while (next != NilTuple) {
        val tuple: Tuple = next.get
        val key: Tuple = keyIndices.map(i => tuple(i))
        aggregates = aggregates.get(key) match {
          case Some(arr: Vector[Tuple]) => aggregates + (key -> (arr :+ tuple))
          case _                        => aggregates + (key -> Vector(tuple))
        }
        next = input.next()
      }

      aggregated = aggregates.toList
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    aggregated match {
      case (key, tuples) :: tail =>
        aggregated = tail
        Some(
          key.++(
            aggCalls.map(agg =>
              tuples.map(t => agg.getArgument(t)).reduce(aggReduce(_, _, agg))
            )
          )
        )
      case _ => NilTuple
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = input.close()
