package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join.getLeftKeys]]
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join.getRightKeys]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  private val leftKeys = getLeftKeys
  private var it = Iterator.empty[Tuple]
  private var mapRight = Map.empty[Tuple, Vector[Tuple]]

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    left.open()

    val rightKeys = getRightKeys

    mapRight = right.foldLeft(Map.empty[Tuple, Vector[Tuple]])((acc, t) => {
      val key = rightKeys.map(t(_))
      val tuples = acc.getOrElse(key, Vector.empty[Tuple])
      acc + (key -> (tuples :+ t))
    })
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (it.hasNext) {
      Some(it.next())
    } else {
      left.next() match {
        case NilTuple => NilTuple
        case Some(t) =>
          it = iterate(t)
          next()
      }
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = left.close()

  private def iterate(l: Tuple): Iterator[Tuple] = {
    mapRight.get(leftKeys.map(l(_))) match {
      case Some(tuples) =>
        (for (e <- tuples)
          yield (l :++ e.asInstanceOf[Tuple])).iterator
      case _ => Iterator.empty
    }
  }
}
