package com.octo.mythbuster.spark.sql.plan.physical

import java.net.URL
import java.nio.file.Path

import com.google.common.base.Charsets
import com.google.common.io.Resources

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

}

// This physical plan parse a CSV in order to produce internal rows
case class CSVFileFullScan(qualifierName: QualifierName, csvFileURL: URL) extends PhysicalPlan with t.LeafTreeNode[PhysicalPlan] {

  import scala.collection.JavaConverters._

  val Separator = ","

  override def execute(): Iterator[InternalRow] = {
    val charSource = Resources.asCharSource(csvFileURL, Charsets.UTF_8)
    val columnNames = charSource.readFirstLine().split(Separator)
    charSource.readLines().asScala.drop(1)
      .map({ line =>
        columnNames.zip(line.split(Separator)).toMap[String, Any]
      })
      .map(_.toInternalRow(qualifierName))
      .iterator
  }

  override def explain(indent: Int = 0): String = {
    s"${"  " * indent}CSVFileFullScan(${csvFileURL}) ${if (supportCodeGeneration()) "*" else ""}"
  }

}

// This physical plan iterate over a iterable in order to produce internal rows
case class IterableFullScan(qualifierName: QualifierName, iterable: Iterable[Row]) extends PhysicalPlan with t.LeafTreeNode[PhysicalPlan] {

  override def execute(): Iterator[InternalRow] = {
    iterable.iterator.map(_.toInternalRow(qualifierName))
  }

  override def explain(indent: Int = 0): String = {
    s"${"  " * indent}IterableFullScan(${qualifierName}) ${if (supportCodeGeneration()) "*" else ""}"
  }

}

// This will filter internal rows which does not fulfill the expression (which should be a predicate)
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

}

// The projection will map the InternalRows by applying the expressions
case class Projection(child: PhysicalPlan, expressions : Seq[e.Expression]) extends PhysicalPlan with t.UnaryTreeNode[PhysicalPlan] with c.CodeGenerationSupport {

  def execute(): Iterator[InternalRow] = child.execute().map({ internalRow: InternalRow =>
    Map(expressions.zipWithIndex.map({ case (expression: e.Expression, index: Int) =>
      val value = expression.evaluate(internalRow)
      (expression match {
        case e.NamedExpression(name) => (None, name)
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

}
