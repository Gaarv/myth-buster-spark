package com.octo.mythbuster.spark.sql.plan.physical

import scala.util.{Failure, Success}
import com.octo.mythbuster.spark.sql._
import com.octo.mythbuster.spark.sql.plan.Plan
import com.octo.mythbuster.spark.sql.plan.physical.codegen.CodeGenerationSupport
import com.octo.mythbuster.spark.sql.{expression => e}
import com.octo.mythbuster.spark.sql.plan.physical.{codegen => c}
import com.octo.mythbuster.spark.{tree => t}

// A PhysicalPlan modelize each stage which will be executed during the execution of the query
trait PhysicalPlan extends Plan[PhysicalPlan] with t.TreeNode[PhysicalPlan] {

  def execute(): Iterator[InternalRow]

  def explain(indent: Int = 0): String

  def supportCodeGeneration(): Boolean = {
    this.isInstanceOf[CodeGenerationSupport]
  }

  def internalFields: Seq[InternalField]

}

// The table scan will produce every Rows contained in the Table as InternalRow
case class TableScan(tableName: TableName, table: Table) extends PhysicalPlan with t.LeafTreeNode[PhysicalPlan] {

  override def execute(): Iterator[InternalRow] = {
    val (_, iterable) = table
    iterable.iterator.map(_.toInternalRow(tableName))
  }

  override def explain(indent: Int = 0): String = {
    s"${"  " * indent}TableScan(${tableName}) ${if (supportCodeGeneration()) "*" else ""}"
  }

  override def internalFields = table.columnNames.map({ columnName => (Some(tableName), columnName) })

}

// The Filter will filter InternalRows which does not fullfill the expression (which should be a predicate)
case class Filter(child: PhysicalPlan, expression: e.Expression) extends PhysicalPlan with c.CodeGenerationSupport with t.UnaryTreeNode[PhysicalPlan] {

  override def execute() : Iterator[InternalRow] = {
    expression.toPredicate match {
      case Success(predicate) => child.execute().filter(predicate.evaluate)
      case Failure(e) => throw e
    }
  }

  override def explain(indent: Int = 0): String = {
    s"""${"  " * indent}Filter(${expression}) ${if (supportCodeGeneration()) "*" else ""}
       |${child.explain(indent + 1)}""".stripMargin
  }

  def internalFields = child.internalFields

  override def doConsumeCode(codeGenerationContext: c.CodeGenerationContext, variableName: c.Code): c.Code = {
    s"""if(!(${expression.generateCode(variableName)})) continue;
       |${consumeCode(codeGenerationContext, variableName)}""".stripMargin
  }

}

// The CartesianProduct will do a cartesian product of the two child (by keeping in memory the right one)
case class CartesianProduct(leftChild: PhysicalPlan, rightChild: PhysicalPlan) extends PhysicalPlan with t.BinaryTreeNode[PhysicalPlan] {

  override def execute(): Iterator[InternalRow] = for {
    leftRow <- leftChild.execute()
    rightRow <- rightChild.execute().toSeq
  } yield leftRow ++ rightRow

  override def explain(indent: Int = 0): String = {
    s"""${"  " * indent}CartesianProduct ${if (supportCodeGeneration()) "*" else ""}
       |${leftChild.explain(indent + 1)}
       |${rightChild.explain(indent + 1)}""".stripMargin
  }

  override def internalFields = leftChild.internalFields ++ rightChild.internalFields

}

// The projection will map the InternalRows by applying the expressions
case class Projection(child: PhysicalPlan, expressions : Seq[e.Expression]) extends PhysicalPlan with t.UnaryTreeNode[PhysicalPlan] with c.CodeGenerationSupport {

  def execute(): Iterator[InternalRow] = child.execute().map({ internalRow: InternalRow =>
    Map(expressions.zipWithIndex.map({ case (expression: e.Expression, index: Int) =>
      val value = expression.evaluate(internalRow)
      (expression match {
        case e.NamedExpression(expressionName) => (None, expressionName)
        case _ => (None, s"column_${index}")
      }) -> value

    }): _*)
  })

  def explain(indent: Int = 0): String = {
    s"""${"  " * indent}Projection(${expressions.mkString(", ")}) ${if (supportCodeGeneration()) "*" else ""}
       |${child.explain(indent + 1)}""".stripMargin
  }

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

  override def internalFields = expressions.zipWithIndex.map({
    case (e.NamedExpression(expressionName), index: Int) => expressionName
    case (_, index) => s"column_${index}"
  }).map({ columnName => (None, columnName) })

}
