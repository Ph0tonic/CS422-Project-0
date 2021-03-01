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
  private var sorted = List[Tuple]()
  protected var sortedIterator: Iterator[Tuple] = Iterator()

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()
    counter = fetch.getOrElse(-1)
    var next = input.next()
    while (next != NilTuple) {
      sorted = next.get :: sorted
      next = input.next()
    }

//    val sortFunction = collation.getFieldCollations()
//    sortFunction.stream().map(x => (t1:Tuple,t2:Tuple) => x.getFieldIndex

//    sorted = sorted.sortBy(collation.getFieldCollations.get(0).get)

//    collation.getFieldCollations
//    offset.get
//    sorted.sortWith() //TODO implement the sort operation
//    sorted.sortBy(t => t.sortBy(collation.getFieldCollations)

    collation.getFieldCollations.stream().map(c => {
      val comparer = if (c.getDirection.isDescending) {
        val a:Tuple = IndexedSeq.empty[Elem]
        val b:Tuple = IndexedSeq.empty
        val d:Elem = a(0)
        val e = d < d

        RelFieldCollation.compare(a(c.getFieldIndex),a(c.getFieldIndex), 0)
        val res = c.compare(a,b,0)

        val comp = (Tuple,Tuple):Boolean = (a,b) => a(index) < b(index)
      } else {
        (a:Tuple,b:Tuple) => a(index) > b(index)
      }
      val index = c.getFieldIndex
      c.getFieldIndex
    })
    collation.getFieldCollations.forEach(c => {
      c.getDirection
      c.getFieldIndex
    })

    sortedIterator = sorted.iterator.drop(offset.getOrElse(0))
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] =
    if (counter != 0 && sortedIterator.hasNext) {
      counter = counter - 1
      Some(sortedIterator.next())
    } else {
      NilTuple
    }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
    sorted = Nil
  }
}
