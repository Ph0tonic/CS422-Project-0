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

  private var lazyRight = LazyList.empty[(Tuple, Seq[Comparable[Elem]])]
  private var lazyJoined = LazyList.empty[Tuple]

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    left.open()
    right.open()

    val keys = getLeftKeys.zip(getRightKeys)

    def next(
        it: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ): LazyList[(Tuple, Seq[Comparable[Elem]])] =
      it.next() match {
        case Some(t) =>
          (t, getRightKeys.map(t(_).asInstanceOf[Comparable[Elem]])) #:: next(
            it
          )
        case NilTuple =>
          it.close()
          LazyList.empty
      }

    lazyRight = next(right)

    def join(
        previousLeft: Tuple,
        comparableLeft: Seq[Comparable[Elem]],
        rightIterator: LazyList[(Tuple, Seq[Comparable[Elem]])]
    ): LazyList[Tuple] =
      rightIterator match {
        case LazyList() =>
          left.next() match {
            case NilTuple => LazyList.empty
            case Some(l)  => join(l, getLeftKeys.map(l(_).asInstanceOf[Comparable[Elem]]), lazyRight)
          }
        case (right, comparableRight) #:: tail
            if comparableLeft.zip(comparableRight).forall {
              case (l, r) => l.compareTo(r) == 0
            } =>
          previousLeft.:++(right) #:: join(previousLeft, comparableLeft, tail)
        case _ #:: tail => join(previousLeft, comparableLeft, tail)
      }

    lazyJoined = left.next() match {
      case NilTuple => LazyList.empty
      case Some(l) =>
        join(l, getLeftKeys.map(l(_).asInstanceOf[Comparable[Elem]]), lazyRight)
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
