package com.octo.mythbuster.spark.sql.catalyst.plans.physical

import com.octo.mythbuster.spark.sql.catalyst.plans.logical
import com.octo.mythbuster.spark.sql.catalyst.plans.logical.LogicalPlan
import com.octo.mythbuster.spark.sql.{ Row, TableName }
import com.octo.mythbuster.spark.sql.catalyst.expressions.Predicate

sealed trait PhysicalPlan {

  def explain(indentCount: Int = 0): String

  def execute(): Iterator[Row]

}

object delegateTo {

  def apply(iteratorFunc: => Iterator[Row])(explainFunc: (Int) => String): PhysicalPlan = {
    new PhysicalPlan {

      def explain(indentCount: Int = 0): String = explainFunc(indentCount)

      def execute() = {
        val iterator = iteratorFunc

        new Iterator[Row] {

          override def hasNext = iterator.hasNext

          override def next() = iterator.next()

        }
      }

    }
  }

}

object IterableScan {

  def apply(iterable: Iterable[Row]): PhysicalPlan = delegateTo({ iterable.iterator }) { indentCount =>
    val indent = "\t" * indentCount
    s"${indent}IterableScan(iterable=[\n${"\t" * (indentCount + 1)}${iterable.map({ row => s"(${row.values.mkString(", ")})" }).mkString(", ")}\n${indent}])"
  }

}

object Filter {

  def apply(child: PhysicalPlan, predicate: Predicate): PhysicalPlan = delegateTo({ child.execute().filter(predicate.evaluate) }) { indentCount =>
    val indent = "\t" * indentCount
    s"${indent}Filter(predicate=${predicate}), \n${child.explain(indentCount + 1)}\n${indent})"
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
  }) { indentCount =>
    val indent = "\t" * indentCount
    s"${indent}CartesianProduct(\n${leftChild.explain(indentCount + 1)},\n${rightChild.explain(indentCount + 1)}\n${indent})"
  }

}

object PhysicalPlan {

  def apply(logicalPlan: LogicalPlan)(implicit iterablesByTableName: Map[TableName, Iterable[Row]]): PhysicalPlan = logicalPlan match {
    case logical.CartesianProduct(leftChild, rightChild) => CartesianProduct(apply(leftChild), apply(rightChild))
    case logical.Filter(child, predicate) => Filter(apply(child), predicate)
    case logical.Scan(tableName: TableName) => IterableScan(iterablesByTableName(tableName))
  }

}
