package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, NilTuple, Tuple}
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

  private var lazyRight = LazyList.empty[Tuple]
  private var lazyJoined = LazyList.empty[Tuple]

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    left.open()
    right.open()

    def next(
              it: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
            ): LazyList[Tuple] =
      it.next() match {
        case Some(tuple) => tuple #:: next(it)
        case NilTuple =>
          it.close()
          LazyList.empty
      }

    lazyRight = next(right)
    val keys = getLeftKeys.zip(getRightKeys)

    def join(
              previousLeft: Tuple,
              rightIterator: LazyList[Tuple]
            ): LazyList[Tuple] =
      rightIterator match {
        case LazyList() =>
          left.next() match {
            case NilTuple => LazyList.empty
            case Some(l)  => join(l, lazyRight)
          }
        case right #:: tail if keys.forall {
          case (leftIndex, rightIndex) =>
            previousLeft(leftIndex)
              .asInstanceOf[Comparable[Elem]]
              .compareTo(
                right(rightIndex).asInstanceOf[Comparable[Elem]]
              ) == 0
        } =>
          previousLeft.:++(right) #:: join(previousLeft, tail)
        case _ #:: tail => join(previousLeft, tail)
      }

    lazyJoined = left.next() match {
      case NilTuple => LazyList.empty
      case Some(l)  => join(l, lazyRight)
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    lazyJoined match {
      case LazyList() => NilTuple
      case tuple #:: tail =>
        lazyJoined = tail
        Some(tuple)
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    left.close()
    right.close()
  }
}
