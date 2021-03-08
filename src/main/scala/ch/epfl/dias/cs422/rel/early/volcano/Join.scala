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

  private var lazyJoined = LazyList.empty[Tuple]

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    left.open()
    right.open()

    def next(
        it: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
        keys: IndexedSeq[Int]
    ): LazyList[(Tuple, Seq[Elem])] =
      it.next() match {
        case Some(tuple) => (tuple, keys.map(tuple(_))) #:: next(it, keys)
        case NilTuple =>
          it.close()
          LazyList.empty
      }

    val lazyLeft = next(left, getLeftKeys)
    val lazyRight = next(right, getRightKeys)

    lazyJoined = for {
      (l, lCompare) <- lazyLeft
      (r, rCompare) <- lazyRight
      if lCompare.zip(rCompare).forall {
        case (c1, c2) =>
          c1.asInstanceOf[Comparable[Elem]]
            .compareTo(c2) == 0
      }
    } yield l.:++(r)
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
