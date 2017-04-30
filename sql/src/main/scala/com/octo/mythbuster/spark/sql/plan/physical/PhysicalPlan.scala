package com.octo.mythbuster.spark.sql.plan.physical

import java.net.URL
import java.nio.file.Path
import java.util

import com.google.common.base.Charsets
import com.google.common.io.Resources
import com.octo.mythbuster.spark.sql.expression.BinaryOperation
import com.octo.mythbuster.spark.sql.plan.physical.codegen.wrapper.{InternalRow => JavaInternalRow}

import scala.util.{Failure, Success}
import com.octo.mythbuster.spark.sql._
import com.octo.mythbuster.spark.sql.plan.Plan
import com.octo.mythbuster.spark.sql.plan.physical.codegen.JavaCodeGenerationSupport
import com.octo.mythbuster.spark.sql.{expression => e}
import com.octo.mythbuster.spark.sql.plan.physical.{codegen => c}
import com.octo.mythbuster.spark.{tree => t}
import com.octo.mythbuster.spark.compiler.JavaCode

// A PhysicalPlan modelize each stage which will be executed during the execution of the query
trait PhysicalPlan extends Plan[PhysicalPlan] with t.TreeNode[PhysicalPlan] {

  def execute(): Iterator[InternalRow]

  def explain(indent: Int = 0): String

  def supportCodeGeneration(): Boolean = {
    this.isInstanceOf[JavaCodeGenerationSupport]
  }

}

// This physical plan parse a CSV in order to produce internal rows
case class CSVFileFullScan(qualifierName: QualifierName, csvFileURL: URL) extends PhysicalPlan with t.LeafTreeNode[PhysicalPlan] {

  import scala.collection.JavaConverters._

  val Separator = ";"

  override def execute(): Iterator[InternalRow] = {
    val charSource = Resources.asCharSource(csvFileURL, Charsets.UTF_8)
    val columnNames = charSource.readFirstLine().split(Separator).map(_.toLowerCase)
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

// This will filter internal rows which does not fulfill the expression (which should be a predicate)
case class Filter(child: PhysicalPlan, expression: e.Expression) extends PhysicalPlan with JavaCodeGenerationSupport with t.UnaryTreeNode[PhysicalPlan] {

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

  override def doConsumeJavaCode(codeGenerationContext: c.JavaCodeGenerationContext, variableName: JavaCode): JavaCode = {
    s"""
       |// FILTER
       |if(!(${expression.generateJavaCode(variableName)})) continue;
       |${consumeJavaCode(codeGenerationContext, variableName)}
      """.stripMargin
  }

}

// The CartesianProduct will do a cartesian product of the two child (by keeping in memory the right one)
case class CartesianProduct(leftChild: PhysicalPlan, rightChild: PhysicalPlan) extends PhysicalPlan with t.BinaryTreeNode[PhysicalPlan] {

  override def execute(): Iterator[InternalRow] = {
    val rightRows = rightChild.execute().toSeq
    for {
      leftRow <- leftChild.execute()
      rightRow <- rightRows
    } yield leftRow ++ rightRow
  }

  override def explain(indent: Int = 0): String = {
    s"""${"  " * indent}CartesianProduct ${if (supportCodeGeneration()) "*" else ""}
       |${leftChild.explain(indent + 1)}
       |${rightChild.explain(indent + 1)}""".stripMargin
  }

}

// The CartesianProduct will do a cartesian product of the two child (by keeping in memory the right one)
case class Join(leftChild: PhysicalPlan, rightChild: PhysicalPlan, operation : BinaryOperation) extends PhysicalPlan with t.BinaryTreeNode[PhysicalPlan] with JavaCodeGenerationSupport {

  override val child = leftChild

  override def execute(): Iterator[InternalRow] = for {
    leftRow <- leftChild.execute()
    rightRow <- rightChild.execute().toSeq if operation.evaluate(leftRow ++ rightRow).asInstanceOf[Boolean]
  } yield leftRow ++ rightRow

  override def explain(indent: Int = 0): String = {
    s"""${"  " * indent}CartesianProduct ${if (supportCodeGeneration()) "*" else ""}
       |${leftChild.explain(indent + 1)}
       |${rightChild.explain(indent + 1)}""".stripMargin
  }

  override def doConsumeJavaCode(codeGenerationContext: c.JavaCodeGenerationContext, variableName: JavaCode): JavaCode = {
    import scala.collection.JavaConverters._
    import com.octo.mythbuster.spark.sql.plan.physical.codegen.Implicits._
    val rightIterable : java.lang.Iterable[JavaInternalRow] = new java.lang.Iterable[JavaInternalRow]() {
      lazy val data : Seq[InternalRow] = rightChild.execute().toSeq

      override def iterator(): util.Iterator[JavaInternalRow] = data.toIterator.wrapForJava()
    }
    val joinedIterableVariable = codeGenerationContext.addReference(rightIterable, "Iterable<InternalRow>")
    val currentIterator =  codeGenerationContext.iteratorVariable
    Iterable().iterator
    s"""
        |// JOIN
        |Iterator<InternalRow> $currentIterator = $joinedIterableVariable.iterator();
        |while($currentIterator.hasNext()) {
        |  InternalRow joinedRow = $currentIterator.next();
        |  if(!((${operation.rightChild.generateJavaCode("joinedRow")}).toString().equals((${operation.leftChild.generateJavaCode(variableName)}).toString()))) continue;
        |  $variableName.concatenate(joinedRow);
        |  ${consumeJavaCode(codeGenerationContext, variableName)}
        |  break;
        |}
        |""".stripMargin
  }

}

// The projection will map the InternalRows by applying the expressions
case class Projection(child: PhysicalPlan, expressions : Seq[e.Expression]) extends PhysicalPlan with t.UnaryTreeNode[PhysicalPlan] with JavaCodeGenerationSupport {

  def execute(): Iterator[InternalRow] = child.execute().map({ internalRow =>
    Map(expressions.zipWithIndex.map({ case (expression: e.Expression, index: Int) =>
      val value: Any = expression.evaluate(internalRow)
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

  override def doConsumeJavaCode(codeGenerationContext: c.JavaCodeGenerationContext, variableName: JavaCode): JavaCode = {
    val internalRowWithProjection = codeGenerationContext.freshVariableName()
    val arrayVariableName = codeGenerationContext.freshVariableName()
    codeGenerationContext.addClassAttribute(
      name = arrayVariableName,
      typeName = "String[]",
      initCode = s"new String[] {${expressions.map(_.generateJavaCode(null)).mkString(",")}}"
    )

    val projections = expressions
      .map(_.generateJavaCode(null))

    val projectionsCode = projections
      .map(c => s"${internalRowWithProjection}.setValue($c, $variableName.getValue($c));")
      .mkString("\n")


    s"""
      |// PROJECTION over ${projections.mkString(",")}
      |InternalRow ${internalRowWithProjection} = InternalRow.create();
      |$projectionsCode
      |currentRows.add(${internalRowWithProjection});
    """.stripMargin
  }

}
