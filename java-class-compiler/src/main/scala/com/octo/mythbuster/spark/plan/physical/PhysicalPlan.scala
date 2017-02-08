package com.octo.mythbuster.spark.plan.physical

import com.octo.mythbuster.spark.plan.logical
import com.octo.mythbuster.spark.plan.logical.LogicalPlan
import com.octo.mythbuster.spark.{ Row, TableName }
import com.octo.mythbuster.spark.expression.Predicate

sealed trait PhysicalPlan {

  def execute(): Iterator[Row]

}

object delegateTo {

  def apply(iteratorFunc: => Iterator[Row])(toStringFunc: => String): PhysicalPlan = {
    new PhysicalPlan {

      def execute() = {
        val iterator = iteratorFunc

        new Iterator[Row] {

          override def hasNext = iterator.hasNext

          override def next() = iterator.next()

        }
      }

      override def toString(): String = toStringFunc

    }
  }

}

object IterableScan {

  def apply(iterable: Iterable[Row]): PhysicalPlan = delegateTo({ iterable.iterator }) {
    s"IterableScan(${iterable})"
  }

}

object Filter {

  def apply(child: PhysicalPlan, predicate: Predicate): PhysicalPlan = delegateTo({ child.execute().filter(predicate.evaluate) }) {
    s"Filter(${child}, ${predicate})"
  }

}

object CartesianProduct {

  def apply(leftChild: PhysicalPlan, rightChild: PhysicalPlan): PhysicalPlan = delegateTo({
    val leftChildIterator = leftChild.execute()
    val rightChildSeq = rightChild.execute().toSeq
    for {
      leftRow <- leftChildIterator
      rightRow <- rightChildSeq
    } yield leftRow ++ rightRow
  }) {
    s"CartesianProduct(${leftChild}, ${rightChild})"
  }

}

object PhysicalPlan {

  def apply(logicalPlan: LogicalPlan)(implicit iterablesByTableName: Map[TableName, Iterable[Row]]): PhysicalPlan = logicalPlan match {
    case logical.CartesianProduct(leftChild, rightChild) => CartesianProduct(apply(leftChild), apply(rightChild))
    case logical.Filter(child, predicate) => Filter(apply(child), predicate)
    case logical.Scan(tableName: TableName) => IterableScan(iterablesByTableName(tableName))
  }

}
