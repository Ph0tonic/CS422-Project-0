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
        case NilTuple    => LazyList.empty
      }

    lazyRight = next(right)
    lazyLeft = next(left)
    val keys = getLeftKeys.zip(getRightKeys)

    def join(
        left: Tuple,
        leftIterator: LazyList[Tuple],
        rightIterator: LazyList[Tuple]
    ): LazyList[Tuple] =
      rightIterator match {
        case LazyList() => {
          leftIterator match {
            case LazyList() => LazyList.empty
            case l #:: tail => join(l, tail, lazyRight)
          }
        }
        case right #:: tail if keys.forall {
              case (i, j) => left(i) == right(j)
            } => (right:++left) #:: join(left, leftIterator, tail)
        case _ #:: tail => join(left, leftIterator, tail)
      }

    lazyJoined = lazyLeft match {
      case LazyList() => LazyList.empty
      case l #:: tail => join(l, tail, lazyRight)
    }
  }

  private var lazyRight = LazyList.empty[Tuple]
  private var lazyLeft = LazyList.empty[Tuple]
  private var lazyJoined = LazyList.empty[Tuple]

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
