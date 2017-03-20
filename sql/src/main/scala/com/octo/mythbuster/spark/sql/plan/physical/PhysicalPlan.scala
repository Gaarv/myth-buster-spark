package com.octo.mythbuster.spark.sql.plan.physical

import scala.util.{ Failure, Success }
import com.octo.mythbuster.spark.sql._
import com.octo.mythbuster.spark.sql.plan.Plan
import com.octo.mythbuster.spark.sql.{ expression => e }
import com.octo.mythbuster.spark.sql.plan.physical.{ codegen => c }
import com.octo.mythbuster.spark.{ tree => t }

trait PhysicalPlan extends Plan[PhysicalPlan] with t.TreeNode[PhysicalPlan] {

  def execute(): Iterator[InternalRow]

}

case class TableScan(tableName: TableName, iterable: Iterable[Row]) extends PhysicalPlan with t.LeafTreeNode[PhysicalPlan] {

  override def execute(): Iterator[InternalRow] = iterable.iterator.map(_.toInternalRow(tableName))
}

case class Filter(child: PhysicalPlan, expression: e.Expression) extends PhysicalPlan with c.CodeGenerationSupport with t.UnaryTreeNode[PhysicalPlan] {

  override def execute() : Iterator[InternalRow] = {
    expression.toPredicate match {
      case Success(predicate) => child.execute().filter(predicate.evaluate)
      case Failure(e) => throw e
    }
  }

  override def doConsumeCode(codeGenerationContext: c.CodeGenerationContext, variableName: c.Code): c.Code = {
    s"""
      |if(!(${expression.generateCode(variableName)})) continue;
      |${consumeCode(codeGenerationContext, variableName)}
    """.stripMargin
  }

}

case class CartesianProduct(leftChild: PhysicalPlan, rightChild: PhysicalPlan) extends PhysicalPlan with t.BinaryTreeNode[PhysicalPlan] {

  override def execute(): Iterator[InternalRow] = for {
    leftRow <- leftChild.execute()
    rightRow <- rightChild.execute().toSeq
  } yield leftRow ++ rightRow
}

case class Projection(child: PhysicalPlan, expressions : Seq[e.Expression]) extends PhysicalPlan with t.UnaryTreeNode[PhysicalPlan] with c.CodeGenerationSupport {

  def execute(): Iterator[InternalRow] = child.execute().map({ physicalRow: InternalRow =>
    Map(expressions.zipWithIndex.map({ case (expression: e.Expression, index: Int) =>
      val value = expression.evaluate(physicalRow)
      (expression match {
        case e.NamedExpression(expressionName) => (None, expressionName)
        case _ => (None, s"column_${index}")
      }) -> value

    }): _*)
  })

  override def doConsumeCode(codeGenerationContext: c.CodeGenerationContext, variableName: c.Code): c.Code = {
    val internalRowWithProjection = codeGenerationContext.freshVariableName()
    val arrayVariableName = codeGenerationContext.freshVariableName()
    s"""Object ${arrayVariableName}[] = {
       |  ${expressions.map(_.generateCode(variableName)).mkString(",\n")}
       |};
       |InternalRow ${internalRowWithProjection} = InternalRow.create();
       |for(int index = 0; index < ${arrayVariableName}.length; index++) {
       |  ${internalRowWithProjection}.setValue(TableNameAndColumnName.of(Optional.empty(), "column_" + index), ${arrayVariableName}[index]);
       |}
       |currentRows.add(${internalRowWithProjection});
    """.stripMargin
  }

}
