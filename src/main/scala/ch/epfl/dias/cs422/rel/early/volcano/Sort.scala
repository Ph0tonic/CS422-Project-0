package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, NilTuple, Tuple}
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Sort protected (
                       input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
                       collation: RelCollation,
                       offset: Option[Int],
                       fetch: Option[Int]
                     ) extends skeleton.Sort[
  ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
](input, collation, offset, fetch)
  with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  private var counter = 0
  private var sorted = Array.empty[Tuple]
  protected var sortedIterator: Iterator[Tuple] = Iterator()

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()
    counter = fetch.getOrElse(-1)
    var next = input.next()
    while (next != NilTuple) {
      sorted = sorted :+ next.get
      next = input.next()
    }

    val ordering: Ordering[Tuple] = collation.getFieldCollations
      .toArray(
        Array.ofDim[RelFieldCollation](collation.getFieldCollations.size)
      )
      .map(c => {
        val order = Ordering.by[Tuple, Comparable[Elem]](
          _(c.getFieldIndex).asInstanceOf[Comparable[Elem]]
        )
        if (c.direction.isDescending) {
          order.reverse
        } else {
          order
        }
      })
      .reduce(_.orElse(_))

    sorted = sorted.sorted(ordering)
    sortedIterator = sorted.iterator.drop(offset.getOrElse(0))
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] =
    if (counter != 0 && sortedIterator.hasNext) {
      if(counter > 0) {
        counter = counter - 1
      }
      Some(sortedIterator.next())
    } else {
      NilTuple
    }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
    sorted = Array.empty[Tuple]
  }
}
